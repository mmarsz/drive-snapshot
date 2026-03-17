#!/usr/bin/env python3
"""
drive-snapshot: Cataloga arquivos de HDs/SSDs que não podem rodar ao mesmo tempo.
Cria snapshots com hashes para encontrar duplicatas entre drives.

Uso:
  ./drive-snapshot.py snapshot <caminho> [--label NOME]   Escaneia um drive montado
  ./drive-snapshot.py list                                 Lista todos os snapshots
  ./drive-snapshot.py files <snapshot_id> [--sort size]    Lista arquivos de um snapshot
  ./drive-snapshot.py search <pattern>                     Busca arquivos por nome
  ./drive-snapshot.py duplicates [--across]                Encontra arquivos duplicados
  ./drive-snapshot.py compare <id1> <id2>                  Compara dois snapshots
  ./drive-snapshot.py export <snapshot_id> [--format csv]  Exporta snapshot
  ./drive-snapshot.py delete <snapshot_id>                 Remove um snapshot
  ./drive-snapshot.py mount <snapshot_id> <mountpoint>     Monta snapshot como pasta virtual (FUSE)
  ./drive-snapshot.py pending [snapshot_id]                Mostra operações pendentes
  ./drive-snapshot.py apply <snapshot_id> <mount_real>     Aplica operações no drive real
"""

import argparse
import hashlib
import json
import logging
import os
import re
import signal
import sqlite3
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

# Dependência opcional: rich para barras de progresso coloridas
try:
    from rich.progress import (
        BarColumn,
        MofNCompleteColumn,
        Progress,
        SpinnerColumn,
        TextColumn,
        TimeRemainingColumn,
    )
    _RICH_AVAILABLE = True
except ImportError:
    _RICH_AVAILABLE = False

# Cores ANSI para output colorido
_COLORS = {
    "red": "\033[31m",
    "yellow": "\033[33m",
    "green": "\033[32m",
    "dim": "\033[2m",
    "reset": "\033[0m",
}
# Desabilita cores se stdout não é terminal
if not sys.stdout.isatty():
    _COLORS = {k: "" for k in _COLORS}

DB_PATH = Path(__file__).parent / "snapshots.db"


# --- Logging ---

def _setup_logging(verbose=False):
    """Configura logging para arquivo e opcionalmente terminal."""
    log_dir = Path.home() / ".local" / "share" / "drive-snapshot"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "drive-snapshot.log"

    handlers = [logging.FileHandler(log_file, encoding="utf-8")]
    if verbose:
        handlers.append(logging.StreamHandler())

    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers,
    )
    return logging.getLogger("drive-snapshot")


# --- Database ---

def get_db():
    db = sqlite3.connect(str(DB_PATH))
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("""
        CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            label TEXT NOT NULL,
            mount_path TEXT NOT NULL,
            created_at TEXT NOT NULL,
            total_files INTEGER DEFAULT 0,
            total_size INTEGER DEFAULT 0,
            status TEXT DEFAULT 'complete'
        )
    """)
    # Migração: adiciona coluna status em bancos antigos que não a possuem
    cols = {row[1] for row in db.execute("PRAGMA table_info(snapshots)").fetchall()}
    if "status" not in cols:
        db.execute("ALTER TABLE snapshots ADD COLUMN status TEXT DEFAULT 'complete'")
    db.execute("""
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_id INTEGER NOT NULL,
            path TEXT NOT NULL,
            size INTEGER NOT NULL,
            mtime REAL,
            sha256 TEXT,
            FOREIGN KEY (snapshot_id) REFERENCES snapshots(id) ON DELETE CASCADE
        )
    """)
    db.execute("CREATE INDEX IF NOT EXISTS idx_files_snapshot ON files(snapshot_id)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_files_hash ON files(sha256)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_files_path ON files(path)")
    db.execute("""
        CREATE TABLE IF NOT EXISTS pending_ops (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_id INTEGER NOT NULL,
            op_type TEXT NOT NULL,
            src_path TEXT NOT NULL,
            dst_path TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY (snapshot_id) REFERENCES snapshots(id) ON DELETE CASCADE
        )
    """)
    db.commit()
    return db


# --- Helpers ---

def hash_file(filepath, chunk_size=1024 * 1024):
    """SHA256 de um arquivo, lendo em chunks para não explodir memória."""
    h = hashlib.sha256()
    try:
        with open(filepath, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                h.update(chunk)
        return h.hexdigest()
    except (PermissionError, OSError):
        return None


def fmt_size(size):
    """Formata bytes para humano."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(size) < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} PB"


def fmt_time(ts):
    """Formata timestamp epoch para string."""
    if ts is None:
        return "?"
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")


# --- Commands ---

def cmd_snapshot(args):
    logger = logging.getLogger("drive-snapshot")
    mount = os.path.abspath(args.path)
    if not os.path.isdir(mount):
        print(f"Erro: '{mount}' não é um diretório válido.", file=sys.stderr)
        sys.exit(1)

    # Auto-detecta label do filesystem via lsblk se --label não fornecido
    label = args.label
    if not label:
        try:
            result = subprocess.run(
                ['lsblk', '-no', 'LABEL', mount],
                capture_output=True, text=True, timeout=5,
            )
            detected = result.stdout.strip()
            if detected:
                label = detected
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass  # lsblk não disponível, usa fallback
    if not label:
        label = os.path.basename(mount) or mount
    print(f"Escaneando: {mount}")
    print(f"Label: {label}")
    logger.info(f"Iniciando snapshot: path={mount} label={label}")

    # Callback para erros de acesso durante os.walk (ex.: nomes não-UTF-8 em NTFS/exFAT)
    def walk_error(err):
        print(f"\n  AVISO: não foi possível acessar: {err.filename} ({err})", file=sys.stderr)
        logger.warning(f"Erro de acesso durante walk: {err.filename} ({err})")

    # Primeira passada: contar arquivos para barra de progresso
    print("Contando arquivos...", end="", flush=True)
    file_list = []
    for root, dirs, filenames in os.walk(mount, onerror=walk_error):
        for fname in filenames:
            fpath = os.path.join(root, fname)
            if os.path.isfile(fpath) and not os.path.islink(fpath):
                file_list.append(fpath)
    total = len(file_list)
    print(f" {total} arquivos encontrados.")

    if total == 0:
        print("Nenhum arquivo encontrado. Nada a fazer.")
        logger.info("Nenhum arquivo encontrado. Snapshot cancelado.")
        return

    db = get_db()

    # Verifica se há snapshot interrompido com mesmo label+mount para retomar
    already_scanned = set()
    existing = db.execute(
        "SELECT id FROM snapshots WHERE label = ? AND mount_path = ? AND status IN ('scanning', 'interrupted')",
        (label, mount),
    ).fetchone()
    if existing:
        snap_id = existing["id"]
        # Carrega paths já escaneados para pular
        already_scanned = {
            r["path"] for r in db.execute(
                "SELECT path FROM files WHERE snapshot_id = ?", (snap_id,)
            ).fetchall()
        }
        print(f"  Retomando snapshot #{snap_id} ({len(already_scanned)} arquivos já escaneados)")
        logger.info(f"Retomando snapshot #{snap_id} com {len(already_scanned)} arquivos já escaneados")
    else:
        cur = db.execute(
            "INSERT INTO snapshots (label, mount_path, created_at, status) VALUES (?, ?, ?, 'scanning')",
            (label, mount, datetime.now().isoformat()),
        )
        snap_id = cur.lastrowid
        db.commit()

    total_size = 0
    hashed = 0
    skipped = 0
    batch = []
    batch_size = 500
    start = time.time()
    _interrupted = False

    # Handler para Ctrl+C: salva progresso e marca como interrompido
    def _handle_sigint(signum, frame):
        nonlocal _interrupted
        _interrupted = True
        print("\n\nInterrompido! Salvando progresso...")
        if batch:
            db.executemany(
                "INSERT INTO files (snapshot_id, path, size, mtime, sha256) VALUES (?, ?, ?, ?, ?)",
                batch,
            )
        db.execute(
            "UPDATE snapshots SET total_files = ?, total_size = ?, status = 'interrupted' WHERE id = ?",
            (total - skipped, total_size, snap_id),
        )
        db.commit()
        logger.info(f"Snapshot #{snap_id} interrompido pelo usuário. Progresso salvo.")
        print(f"  Snapshot #{snap_id} salvo como 'interrompido'. Use 'snapshot' novamente para retomar.")
        sys.exit(130)

    old_handler = signal.signal(signal.SIGINT, _handle_sigint)

    # Função de progresso com rich (se disponível) ou fallback \r
    def _run_scan_loop():
        nonlocal total_size, hashed, skipped, batch

        if _RICH_AVAILABLE:
            with Progress(
                SpinnerColumn(),
                TextColumn("[bold blue]{task.description}"),
                BarColumn(),
                MofNCompleteColumn(),
                TextColumn("•"),
                TextColumn("{task.fields[size]}"),
                TextColumn("•"),
                TimeRemainingColumn(),
            ) as progress:
                task = progress.add_task("Escaneando", total=total, size="0 B")
                for i, fpath in enumerate(file_list, 1):
                    _process_file(fpath, i)
                    progress.update(task, advance=1, size=fmt_size(total_size))
        else:
            for i, fpath in enumerate(file_list, 1):
                _process_file(fpath, i)
                if i % 100 == 0 or i == total:
                    elapsed = time.time() - start
                    rate = i / elapsed if elapsed > 0 else 0
                    eta = (total - i) / rate if rate > 0 else 0
                    print(
                        f"\r  [{i}/{total}] {i*100//total}% | "
                        f"{fmt_size(total_size)} | "
                        f"{rate:.0f} arq/s | "
                        f"ETA: {eta:.0f}s   ",
                        end="",
                        flush=True,
                    )

    def _process_file(fpath, i):
        nonlocal total_size, hashed, skipped, batch
        try:
            relpath = os.path.relpath(fpath, mount)
            # Pula arquivos já escaneados (resume)
            if relpath in already_scanned:
                return

            stat_info = os.stat(fpath)
            size = stat_info.st_size
            mtime = stat_info.st_mtime

            if args.no_hash:
                sha = None
            else:
                sha = hash_file(fpath)
                if sha:
                    hashed += 1

            total_size += size
            batch.append((snap_id, relpath, size, mtime, sha))
        except (UnicodeDecodeError, OSError) as e:
            logger.debug(f"Arquivo ignorado (inacessível): {fpath} — {e}")
            skipped += 1
            return

        if len(batch) >= batch_size:
            db.executemany(
                "INSERT INTO files (snapshot_id, path, size, mtime, sha256) VALUES (?, ?, ?, ?, ?)",
                batch,
            )
            db.commit()
            batch.clear()

    _run_scan_loop()

    # Restaura handler original de SIGINT
    signal.signal(signal.SIGINT, old_handler)

    # Resto do batch
    if batch:
        db.executemany(
            "INSERT INTO files (snapshot_id, path, size, mtime, sha256) VALUES (?, ?, ?, ?, ?)",
            batch,
        )

    db.execute(
        "UPDATE snapshots SET total_files = ?, total_size = ?, status = 'complete' WHERE id = ?",
        (total - skipped, total_size, snap_id),
    )
    db.commit()

    elapsed = time.time() - start
    print(f"\n\nSnapshot #{snap_id} criado!")
    print(f"  Arquivos: {total - skipped} ({skipped} inacessíveis)")
    print(f"  Tamanho total: {fmt_size(total_size)}")
    print(f"  Hasheados: {hashed}")
    print(f"  Tempo: {elapsed:.1f}s")
    logger.info(
        f"Snapshot #{snap_id} concluído: {total - skipped} arquivos, "
        f"{fmt_size(total_size)}, {skipped} inacessíveis, {elapsed:.1f}s"
    )


def cmd_list(args):
    db = get_db()
    rows = db.execute(
        "SELECT * FROM snapshots ORDER BY created_at DESC"
    ).fetchall()

    if not rows:
        print("Nenhum snapshot encontrado. Use 'snapshot <caminho>' primeiro.")
        return

    print(f"{'ID':>4}  {'Label':<20}  {'Arquivos':>10}  {'Tamanho':>10}  {'Data':>20}  Mount")
    print("-" * 95)
    for r in rows:
        # Determina cor pela idade do snapshot
        created = datetime.fromisoformat(r['created_at'][:19])
        age = datetime.now() - created
        if age < timedelta(days=7):
            color = _COLORS["green"]      # recente
        elif age < timedelta(days=30):
            color = ""                    # normal (sem cor)
        elif age < timedelta(days=90):
            color = _COLORS["yellow"]     # antigo
        else:
            color = _COLORS["red"]        # muito antigo
        reset = _COLORS["reset"] if color else ""
        print(
            f"{color}{r['id']:>4}  {r['label']:<20}  {r['total_files']:>10}  "
            f"{fmt_size(r['total_size']):>10}  {r['created_at'][:19]:>20}  {r['mount_path']}{reset}"
        )


def cmd_files(args):
    db = get_db()
    snap = db.execute("SELECT * FROM snapshots WHERE id = ?", (args.snapshot_id,)).fetchone()
    if not snap:
        print(f"Snapshot #{args.snapshot_id} não encontrado.", file=sys.stderr)
        sys.exit(1)

    order = "size DESC" if args.sort == "size" else "path ASC"
    limit_clause = f"LIMIT {args.limit}" if args.limit else ""

    rows = db.execute(
        f"SELECT path, size, mtime, sha256 FROM files WHERE snapshot_id = ? ORDER BY {order} {limit_clause}",
        (args.snapshot_id,),
    ).fetchall()

    print(f"Snapshot #{snap['id']} - {snap['label']} ({snap['total_files']} arquivos, {fmt_size(snap['total_size'])})")
    print(f"Mount: {snap['mount_path']}")
    print()

    for r in rows:
        sha_short = r["sha256"][:12] + "…" if r["sha256"] else "sem-hash"
        print(f"  {fmt_size(r['size']):>10}  {fmt_time(r['mtime'])}  {sha_short}  {r['path']}")


def cmd_search(args):
    db = get_db()

    if getattr(args, 'regex', False):
        # Busca com regex via função personalizada no SQLite
        try:
            regex = re.compile(args.pattern, re.IGNORECASE)
        except re.error as e:
            print(f"Regex inválida: {e}", file=sys.stderr)
            sys.exit(1)
        db.create_function("regexp", 2, lambda pattern, string: bool(regex.search(string)) if string else False)
        rows = db.execute(
            """
            SELECT f.path, f.size, f.sha256, s.id as snap_id, s.label
            FROM files f JOIN snapshots s ON f.snapshot_id = s.id
            WHERE regexp(?, f.path)
            ORDER BY f.path
            LIMIT 200
            """,
            (args.pattern,),
        ).fetchall()
        mode = "regex"
    else:
        # Busca padrão com LIKE
        pattern = f"%{args.pattern}%"
        rows = db.execute(
            """
            SELECT f.path, f.size, f.sha256, s.id as snap_id, s.label
            FROM files f JOIN snapshots s ON f.snapshot_id = s.id
            WHERE f.path LIKE ?
            ORDER BY f.path
            LIMIT 200
            """,
            (pattern,),
        ).fetchall()
        mode = "LIKE"

    if not rows:
        print(f"Nenhum arquivo encontrado com '{args.pattern}'.")
        return

    print(f"Resultados ({mode}) para '{args.pattern}' ({len(rows)} encontrados):\n")
    for r in rows:
        sha_short = r["sha256"][:12] + "…" if r["sha256"] else ""
        print(f"  [{r['label']}#{r['snap_id']}]  {fmt_size(r['size']):>10}  {sha_short}  {r['path']}")


def cmd_duplicates(args):
    db = get_db()

    if args.across:
        # Duplicatas ENTRE snapshots diferentes
        query = """
            SELECT sha256, size, COUNT(*) as cnt, COUNT(DISTINCT snapshot_id) as snap_cnt
            FROM files
            WHERE sha256 IS NOT NULL
            GROUP BY sha256
            HAVING COUNT(DISTINCT snapshot_id) > 1
            ORDER BY size DESC
        """
        desc = "entre drives diferentes"
    else:
        # Todas as duplicatas (mesmo drive ou entre drives)
        query = """
            SELECT sha256, size, COUNT(*) as cnt, COUNT(DISTINCT snapshot_id) as snap_cnt
            FROM files
            WHERE sha256 IS NOT NULL
            GROUP BY sha256
            HAVING COUNT(*) > 1
            ORDER BY size DESC
        """
        desc = "em todos os snapshots"

    limit = args.limit or 50
    rows = db.execute(query).fetchall()

    if not rows:
        print(f"Nenhuma duplicata encontrada {desc}.")
        return

    total_wasted = sum(r["size"] * (r["cnt"] - 1) for r in rows)
    total_copies = sum(r["cnt"] - 1 for r in rows)
    print(f"Duplicatas {desc}: {len(rows)} grupos, {total_copies} cópias extras ({fmt_size(total_wasted)} desperdiçado)\n")

    # Busca todas as localizações de uma vez (evita N+1 queries)
    display_rows = rows[:limit]
    all_hashes = [r["sha256"] for r in display_rows]
    locs_by_hash = defaultdict(list)
    if all_hashes:
        placeholders = ",".join("?" * len(all_hashes))
        locations = db.execute(f"""
            SELECT f.sha256, f.path, s.label, s.id as snap_id
            FROM files f JOIN snapshots s ON f.snapshot_id = s.id
            WHERE f.sha256 IN ({placeholders})
            ORDER BY f.sha256, s.label, f.path
        """, all_hashes).fetchall()
        for loc in locations:
            locs_by_hash[loc["sha256"]].append(loc)

    shown = 0
    for r in display_rows:
        print(f"  Hash: {r['sha256'][:16]}…  Tamanho: {fmt_size(r['size'])}  Cópias: {r['cnt']} (em {r['snap_cnt']} snapshot(s))")
        for loc in locs_by_hash[r["sha256"]]:
            print(f"    [{loc['label']}#{loc['snap_id']}] {loc['path']}")
        print()
        shown += 1

    if len(rows) > limit:
        print(f"  ... e mais {len(rows) - limit} grupos. Use --limit para ver mais.")


def cmd_compare(args):
    db = get_db()
    s1 = db.execute("SELECT * FROM snapshots WHERE id = ?", (args.id1,)).fetchone()
    s2 = db.execute("SELECT * FROM snapshots WHERE id = ?", (args.id2,)).fetchone()

    if not s1 or not s2:
        print("Snapshot não encontrado.", file=sys.stderr)
        sys.exit(1)

    print(f"Comparando: #{s1['id']} {s1['label']}  vs  #{s2['id']} {s2['label']}\n")

    # Arquivos por hash em cada snapshot
    def get_hashes(snap_id):
        rows = db.execute(
            "SELECT sha256, path, size FROM files WHERE snapshot_id = ? AND sha256 IS NOT NULL",
            (snap_id,),
        ).fetchall()
        by_hash = {}
        for r in rows:
            by_hash.setdefault(r["sha256"], []).append(r)
        return by_hash

    h1 = get_hashes(args.id1)
    h2 = get_hashes(args.id2)

    set1 = set(h1.keys())
    set2 = set(h2.keys())

    common = set1 & set2
    only1 = set1 - set2
    only2 = set2 - set1

    # Resumo
    common_size = sum(h1[h][0]["size"] for h in common)
    only1_size = sum(h1[h][0]["size"] for h in only1)
    only2_size = sum(h2[h][0]["size"] for h in only2)

    print(f"  Em comum (mesmo conteúdo):   {len(common):>6} arquivos  ({fmt_size(common_size)})")
    print(f"  Só em #{s1['id']} ({s1['label']}):     {len(only1):>6} arquivos  ({fmt_size(only1_size)})")
    print(f"  Só em #{s2['id']} ({s2['label']}):     {len(only2):>6} arquivos  ({fmt_size(only2_size)})")

    # Arquivos com mesmo path mas conteúdo diferente
    paths1 = {r["path"]: r for rows in h1.values() for r in rows}
    paths2 = {r["path"]: r for rows in h2.values() for r in rows}
    common_paths = set(paths1.keys()) & set(paths2.keys())
    changed = [(p, paths1[p], paths2[p]) for p in common_paths if paths1[p]["sha256"] != paths2[p]["sha256"]]

    if changed:
        print(f"\n  Mesmo caminho, conteúdo diferente: {len(changed)}")
        for p, f1, f2 in changed[:20]:
            print(f"    {p}")
            print(f"      #{s1['id']}: {fmt_size(f1['size'])}  {f1['sha256'][:12]}…")
            print(f"      #{s2['id']}: {fmt_size(f2['size'])}  {f2['sha256'][:12]}…")
        if len(changed) > 20:
            print(f"    ... e mais {len(changed) - 20}")


def cmd_export(args):
    db = get_db()
    snap = db.execute("SELECT * FROM snapshots WHERE id = ?", (args.snapshot_id,)).fetchone()
    if not snap:
        print(f"Snapshot #{args.snapshot_id} não encontrado.", file=sys.stderr)
        sys.exit(1)

    rows = db.execute(
        "SELECT path, size, mtime, sha256 FROM files WHERE snapshot_id = ? ORDER BY path",
        (args.snapshot_id,),
    ).fetchall()

    if args.format == "csv":
        import csv
        outfile = f"snapshot_{args.snapshot_id}_{snap['label']}.csv"
        with open(outfile, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["path", "size", "mtime", "sha256"])
            for r in rows:
                w.writerow([r["path"], r["size"], r["mtime"], r["sha256"]])
        print(f"Exportado: {outfile}")
    else:
        outfile = f"snapshot_{args.snapshot_id}_{snap['label']}.json"
        data = {
            "snapshot": dict(snap),
            "files": [dict(r) for r in rows],
        }
        with open(outfile, "w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"Exportado: {outfile}")


def cmd_mount(args):
    logger = logging.getLogger("drive-snapshot")
    try:
        from fuse import FUSE, FuseOSError, Operations
    except ImportError:
        print("Erro: pacote 'fusepy' não instalado. Rode: pip install fusepy", file=sys.stderr)
        sys.exit(1)

    import errno
    import stat as stat_mod
    import threading

    db = get_db()
    snap = db.execute("SELECT * FROM snapshots WHERE id = ?", (args.snapshot_id,)).fetchone()
    if not snap:
        print(f"Snapshot #{args.snapshot_id} não encontrado.", file=sys.stderr)
        sys.exit(1)

    mountpoint = os.path.abspath(args.mountpoint)
    os.makedirs(mountpoint, exist_ok=True)

    class SnapshotFS(Operations):
        def __init__(self, snapshot_id):
            self.snap_id = snapshot_id
            self.lock = threading.Lock()
            self._build_tree()

        def _get_db(self):
            return get_db()

        def _build_tree(self):
            """Constrói árvore de diretórios a partir do banco."""
            db = self._get_db()
            rows = db.execute(
                "SELECT id, path, size, mtime FROM files WHERE snapshot_id = ?",
                (self.snap_id,),
            ).fetchall()

            self.tree = {}    # path -> {type, children, size, mtime, file_id}
            self.tree[""] = {"type": "dir", "children": set(), "size": 0, "mtime": time.time()}

            for r in rows:
                parts = r["path"].split("/")
                # Cria diretórios intermediários
                for i in range(len(parts) - 1):
                    dirpath = "/".join(parts[: i + 1])
                    parent = "/".join(parts[:i]) if i > 0 else ""
                    if dirpath not in self.tree:
                        self.tree[dirpath] = {"type": "dir", "children": set(), "size": 0, "mtime": time.time()}
                        self.tree[parent]["children"].add(parts[i])

                # Arquivo
                filepath = r["path"]
                parent = "/".join(parts[:-1]) if len(parts) > 1 else ""
                self.tree[filepath] = {
                    "type": "file",
                    "size": r["size"],
                    "mtime": r["mtime"] or time.time(),
                    "file_id": r["id"],
                }
                if parent in self.tree:
                    self.tree[parent]["children"].add(parts[-1])

        def _resolve(self, path):
            """Converte path FUSE (começa com /) para chave da árvore."""
            p = path.lstrip("/")
            return p

        def _record_op(self, op_type, src, dst=None):
            db = self._get_db()
            db.execute(
                "INSERT INTO pending_ops (snapshot_id, op_type, src_path, dst_path, created_at) VALUES (?, ?, ?, ?, ?)",
                (self.snap_id, op_type, src, dst, datetime.now().isoformat()),
            )
            db.commit()

        def getattr(self, path, fh=None):
            p = self._resolve(path)
            node = self.tree.get(p)
            if node is None:
                raise FuseOSError(errno.ENOENT)

            now = time.time()
            if node["type"] == "dir":
                return {
                    "st_mode": stat_mod.S_IFDIR | 0o755,
                    "st_nlink": 2 + len(node.get("children", [])),
                    "st_size": 4096,
                    "st_atime": now,
                    "st_mtime": node.get("mtime", now),
                    "st_ctime": node.get("mtime", now),
                    "st_uid": os.getuid(),
                    "st_gid": os.getgid(),
                }
            else:
                return {
                    "st_mode": stat_mod.S_IFREG | 0o644,
                    "st_nlink": 1,
                    "st_size": node["size"],
                    "st_atime": now,
                    "st_mtime": node.get("mtime", now),
                    "st_ctime": node.get("mtime", now),
                    "st_uid": os.getuid(),
                    "st_gid": os.getgid(),
                }

        def readdir(self, path, fh):
            p = self._resolve(path)
            node = self.tree.get(p)
            if node is None or node["type"] != "dir":
                raise FuseOSError(errno.ENOENT)
            return [".", ".."] + list(node.get("children", []))

        def read(self, path, size, offset, fh):
            # Sem conteúdo real — retorna bytes vazios do tamanho reportado
            p = self._resolve(path)
            node = self.tree.get(p)
            if node is None:
                raise FuseOSError(errno.ENOENT)
            # Retorna zeros para não confundir aplicações que tentam ler
            remaining = max(0, node["size"] - offset)
            return b"\x00" * min(size, remaining, 65536)  # limita a 64KB por chamada para evitar OOM em arquivos grandes (ex: 4GB)

        def rename(self, old, new):
            with self.lock:
                old_p = self._resolve(old)
                new_p = self._resolve(new)

                if old_p not in self.tree:
                    raise FuseOSError(errno.ENOENT)

                node = self.tree.pop(old_p)

                # Atualiza parent antigo
                old_parts = old_p.split("/")
                old_parent = "/".join(old_parts[:-1]) if len(old_parts) > 1 else ""
                if old_parent in self.tree:
                    self.tree[old_parent]["children"].discard(old_parts[-1])

                # Cria parent novo se necessário
                new_parts = new_p.split("/")
                new_parent = "/".join(new_parts[:-1]) if len(new_parts) > 1 else ""
                if new_parent and new_parent not in self.tree:
                    self.tree[new_parent] = {"type": "dir", "children": set(), "size": 0, "mtime": time.time()}
                if new_parent in self.tree:
                    self.tree[new_parent]["children"].add(new_parts[-1])

                self.tree[new_p] = node

                # Se é diretório, move todos os filhos também
                if node["type"] == "dir":
                    prefix = old_p + "/"
                    to_move = [(k, v) for k, v in self.tree.items() if k.startswith(prefix)]
                    for k, v in to_move:
                        del self.tree[k]
                        new_k = new_p + "/" + k[len(prefix):]
                        self.tree[new_k] = v

                # Atualiza DB
                db = self._get_db()
                if node["type"] == "file":
                    db.execute("UPDATE files SET path = ? WHERE id = ?", (new_p, node["file_id"]))
                else:
                    prefix = old_p + "/"
                    rows = db.execute(
                        "SELECT id, path FROM files WHERE snapshot_id = ? AND (path = ? OR path LIKE ?)",
                        (self.snap_id, old_p, prefix + "%"),
                    ).fetchall()
                    for r in rows:
                        updated = new_p + r["path"][len(old_p):]
                        db.execute("UPDATE files SET path = ? WHERE id = ?", (updated, r["id"]))

                self._record_op("move", old_p, new_p)
                db.commit()

        def unlink(self, path):
            with self.lock:
                p = self._resolve(path)
                node = self.tree.get(p)
                if node is None:
                    raise FuseOSError(errno.ENOENT)

                del self.tree[p]

                # Atualiza parent
                parts = p.split("/")
                parent = "/".join(parts[:-1]) if len(parts) > 1 else ""
                if parent in self.tree:
                    self.tree[parent]["children"].discard(parts[-1])

                # Atualiza DB
                db = self._get_db()
                if node.get("file_id"):
                    db.execute("DELETE FROM files WHERE id = ?", (node["file_id"],))
                self._record_op("delete", p)
                db.commit()

        def rmdir(self, path):
            with self.lock:
                p = self._resolve(path)
                node = self.tree.get(p)
                if node is None or node["type"] != "dir":
                    raise FuseOSError(errno.ENOENT)
                if node.get("children"):
                    raise FuseOSError(errno.ENOTEMPTY)

                del self.tree[p]

                parts = p.split("/")
                parent = "/".join(parts[:-1]) if len(parts) > 1 else ""
                if parent in self.tree:
                    self.tree[parent]["children"].discard(parts[-1])

                self._record_op("rmdir", p)

        def mkdir(self, path, mode):
            with self.lock:
                p = self._resolve(path)
                if p in self.tree:
                    raise FuseOSError(errno.EEXIST)

                parts = p.split("/")
                parent = "/".join(parts[:-1]) if len(parts) > 1 else ""
                if parent not in self.tree:
                    raise FuseOSError(errno.ENOENT)

                self.tree[p] = {"type": "dir", "children": set(), "size": 0, "mtime": time.time()}
                self.tree[parent]["children"].add(parts[-1])
                self._record_op("mkdir", p)

        # Stubs para FUSE não reclamar
        def open(self, path, flags):
            return 0

        def release(self, path, fh):
            return 0

        def statfs(self, path):
            return {"f_bsize": 4096, "f_blocks": 0, "f_bavail": 0, "f_bfree": 0, "f_files": 0, "f_ffree": 0}

        def chmod(self, path, mode):
            return 0

        def chown(self, path, uid, gid):
            return 0

        def utimens(self, path, times=None):
            return 0

        def truncate(self, path, length, fh=None):
            return 0

        def write(self, path, data, offset, fh):
            raise FuseOSError(errno.EROFS)

        def create(self, path, mode, fi=None):
            raise FuseOSError(errno.EROFS)

    print(f"Montando snapshot #{snap['id']} '{snap['label']}' em {mountpoint}")
    print(f"  {snap['total_files']} arquivos, {fmt_size(snap['total_size'])}")
    print()
    print("  Você pode abrir no Nautilus/file manager normalmente.")
    print("  Renomear e mover arquivos funciona — são gravados como operações pendentes.")
    print("  Deletar funciona — remove do snapshot e grava operação pendente.")
    print("  Conteúdo dos arquivos NÃO está disponível (os drives estão desconectados).")
    print()
    print(f"  Para desmontar: fusermount -u {mountpoint}")
    print()
    logger.info(f"Montando snapshot #{snap['id']} '{snap['label']}' em {mountpoint}")

    FUSE(SnapshotFS(args.snapshot_id), mountpoint, foreground=True, allow_other=False, nothreads=False)

    # Chegamos aqui após desmontagem (FUSE bloqueante)
    logger.info(f"Snapshot #{snap['id']} desmontado de {mountpoint}")


def cmd_pending(args):
    db = get_db()
    if args.snapshot_id:
        rows = db.execute(
            """
            SELECT p.*, s.label FROM pending_ops p
            JOIN snapshots s ON p.snapshot_id = s.id
            WHERE p.snapshot_id = ?
            ORDER BY p.created_at
            """,
            (args.snapshot_id,),
        ).fetchall()
    else:
        rows = db.execute(
            """
            SELECT p.*, s.label FROM pending_ops p
            JOIN snapshots s ON p.snapshot_id = s.id
            ORDER BY p.snapshot_id, p.created_at
            """,
        ).fetchall()

    if not rows:
        print("Nenhuma operação pendente.")
        return

    print(f"Operações pendentes ({len(rows)}):\n")
    for r in rows:
        if r["op_type"] == "move":
            print(f"  [{r['label']}#{r['snapshot_id']}] MOVER: {r['src_path']}")
            print(f"       → {r['dst_path']}")
        elif r["op_type"] == "delete":
            print(f"  [{r['label']}#{r['snapshot_id']}] DELETAR: {r['src_path']}")
        elif r["op_type"] == "mkdir":
            print(f"  [{r['label']}#{r['snapshot_id']}] CRIAR DIR: {r['src_path']}")
        elif r["op_type"] == "rmdir":
            print(f"  [{r['label']}#{r['snapshot_id']}] REMOVER DIR: {r['src_path']}")
        print()


def _validate_path(mount_real, relpath):
    """Valida que relpath não escapa de mount_real (prevenção de path traversal).

    Junta mount_real com relpath, resolve symlinks/.. via realpath e confirma
    que o resultado ainda está contido dentro de mount_real.

    Levanta ValueError se o caminho resolvido escapar do diretório base.
    """
    # Resolve o caminho base uma vez para comparação consistente
    base = os.path.realpath(mount_real)
    # lstrip garante que caminhos absolutos sejam tratados como relativos,
    # evitando que os.path.join ignore o base quando relpath começa com '/'
    resolved = os.path.realpath(os.path.join(base, relpath.lstrip("/")))
    # O separador no final evita falso positivo: /mnt/foo não deve casar com /mnt/foobar
    if not resolved.startswith(base + os.sep) and resolved != base:
        raise ValueError(f"path escapa do mount: {relpath!r}")
    return resolved


def cmd_apply(args):
    logger = logging.getLogger("drive-snapshot")
    db = get_db()
    snap = db.execute("SELECT * FROM snapshots WHERE id = ?", (args.snapshot_id,)).fetchone()
    if not snap:
        print(f"Snapshot #{args.snapshot_id} não encontrado.", file=sys.stderr)
        sys.exit(1)

    mount_real = os.path.abspath(args.mount_path)
    if not os.path.isdir(mount_real):
        print(f"Erro: '{mount_real}' não é um diretório válido.", file=sys.stderr)
        sys.exit(1)

    snap_id = args.snapshot_id

    # Ordena por prioridade de tipo (mkdir antes de move, delete antes de rmdir)
    # para garantir que diretórios sejam criados antes de mover arquivos para dentro deles
    rows = db.execute(
        """SELECT * FROM pending_ops WHERE snapshot_id = ?
           ORDER BY CASE op_type
             WHEN 'mkdir'  THEN 1
             WHEN 'move'   THEN 2
             WHEN 'delete' THEN 3
             WHEN 'rmdir'  THEN 4
             ELSE 5
           END, created_at""",
        (args.snapshot_id,),
    ).fetchall()

    if not rows:
        print("Nenhuma operação pendente para este snapshot.")
        return

    print(f"Aplicando {len(rows)} operações em {mount_real}...")
    print(f"Snapshot: #{snap['id']} '{snap['label']}'")
    print()

    # Preview
    for r in rows:
        if r["op_type"] == "move":
            print(f"  MOVER: {r['src_path']} → {r['dst_path']}")
        elif r["op_type"] == "delete":
            print(f"  DELETAR: {r['src_path']}")
        elif r["op_type"] == "mkdir":
            print(f"  CRIAR DIR: {r['src_path']}")
        elif r["op_type"] == "rmdir":
            print(f"  REMOVER DIR: {r['src_path']}")

    print()
    # Modo dry-run: exibe o preview e sai sem executar nem pedir confirmação
    if args.dry_run:
        print("Modo --dry-run: nenhuma operação executada.")
        return

    confirm = input("Aplicar? [s/N] ")
    if confirm.lower() != "s":
        print("Cancelado.")
        return

    import shutil

    ok = 0
    erros = 0
    conflitos = 0  # destino já existe antes do move
    for r in rows:
        op_type = r["op_type"]
        src_path = r["src_path"]
        dst_path = r["dst_path"]
        try:
            if op_type == "mkdir":
                # Valida src_path antes de criar diretório
                try:
                    target = _validate_path(mount_real, src_path)
                except ValueError:
                    print(f"ERRO: path inválido (escapa mount): {src_path}")
                    logger.warning(f"APPLY #{snap_id}: mkdir {src_path} FAILED: path escapa do mount")
                    erros += 1
                    continue
                os.makedirs(target, exist_ok=True)
                print(f"  OK mkdir: {src_path}")
                logger.info(f"APPLY #{snap_id}: mkdir {src_path} -> {target} OK")
            elif op_type == "move":
                # Valida src_path e dst_path antes de mover
                try:
                    src = _validate_path(mount_real, src_path)
                    dst = _validate_path(mount_real, dst_path)
                except ValueError:
                    # Reporta qual dos dois caminhos disparou a violação
                    print(f"ERRO: path inválido (escapa mount): {src_path} → {dst_path}")
                    logger.warning(f"APPLY #{snap_id}: move {src_path} -> {dst_path} FAILED: path escapa do mount")
                    erros += 1
                    continue
                # Proteção contra sobrescrita silenciosa: se o destino já existe,
                # aborta a operação e registra como conflito
                if os.path.exists(dst):
                    print(f"  CONFLITO: destino já existe: {dst_path} (pulando)")
                    logger.warning(f"APPLY #{snap_id}: move {src_path} -> {dst_path} CONFLITO: destino já existe")
                    conflitos += 1
                    continue
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                shutil.move(src, dst)
                print(f"  OK move: {src_path} → {dst_path}")
                logger.info(f"APPLY #{snap_id}: move {src_path} -> {dst_path} OK")
            elif op_type == "delete":
                # Valida src_path antes de deletar arquivo
                try:
                    target = _validate_path(mount_real, src_path)
                except ValueError:
                    print(f"ERRO: path inválido (escapa mount): {src_path}")
                    logger.warning(f"APPLY #{snap_id}: delete {src_path} FAILED: path escapa do mount")
                    erros += 1
                    continue
                if os.path.isfile(target):
                    os.remove(target)
                    print(f"  OK delete: {src_path}")
                    logger.info(f"APPLY #{snap_id}: delete {src_path} -> {target} OK")
                else:
                    print(f"  SKIP (não encontrado): {src_path}")
            elif op_type == "rmdir":
                # Valida src_path antes de remover diretório
                try:
                    target = _validate_path(mount_real, src_path)
                except ValueError:
                    print(f"ERRO: path inválido (escapa mount): {src_path}")
                    logger.warning(f"APPLY #{snap_id}: rmdir {src_path} FAILED: path escapa do mount")
                    erros += 1
                    continue
                if os.path.isdir(target):
                    os.rmdir(target)
                    print(f"  OK rmdir: {src_path}")
                    logger.info(f"APPLY #{snap_id}: rmdir {src_path} -> {target} OK")
                else:
                    print(f"  SKIP (não encontrado): {src_path}")
            ok += 1
        except Exception as e:
            print(f"  ERRO: {op_type} {src_path}: {e}")
            logger.warning(f"APPLY #{snap_id}: {op_type} {src_path} FAILED: {e}")
            erros += 1

    # Limpa operações aplicadas
    db.execute("DELETE FROM pending_ops WHERE snapshot_id = ?", (args.snapshot_id,))
    db.commit()

    print(f"\nConcluído: {ok} ok, {erros} erros, {conflitos} conflitos.")
    logger.info(f"APPLY #{snap_id}: concluído - {ok} ok, {erros} erros, {conflitos} conflitos")


def cmd_delete(args):
    logger = logging.getLogger("drive-snapshot")
    db = get_db()
    snap = db.execute("SELECT * FROM snapshots WHERE id = ?", (args.snapshot_id,)).fetchone()
    if not snap:
        print(f"Snapshot #{args.snapshot_id} não encontrado.", file=sys.stderr)
        sys.exit(1)

    confirm = input(f"Deletar snapshot #{snap['id']} '{snap['label']}' ({snap['total_files']} arquivos)? [s/N] ")
    if confirm.lower() != "s":
        print("Cancelado.")
        return

    db.execute("DELETE FROM files WHERE snapshot_id = ?", (args.snapshot_id,))
    db.execute("DELETE FROM snapshots WHERE id = ?", (args.snapshot_id,))
    db.commit()
    print(f"Snapshot #{args.snapshot_id} removido.")
    logger.info(f"Snapshot #{args.snapshot_id} '{snap['label']}' removido ({snap['total_files']} arquivos)")


# --- CLI ---

def main():
    parser = argparse.ArgumentParser(
        description="Cataloga arquivos de drives para comparação offline.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    # Flag global de verbose — funciona com todos os subcomandos
    parser.add_argument("--verbose", "-v", action="store_true", help="Mostra logs detalhados no terminal")
    sub = parser.add_subparsers(dest="command")

    # snapshot
    p = sub.add_parser("snapshot", help="Escaneia um drive montado")
    p.add_argument("path", help="Caminho do ponto de montagem")
    p.add_argument("--label", "-l", help="Nome/label do drive (ex: 'HD-Fotos')")
    p.add_argument("--no-hash", action="store_true", help="Pula o cálculo de SHA256 (mais rápido)")

    # list
    sub.add_parser("list", help="Lista snapshots")

    # files
    p = sub.add_parser("files", help="Lista arquivos de um snapshot")
    p.add_argument("snapshot_id", type=int)
    p.add_argument("--sort", choices=["path", "size"], default="path")
    p.add_argument("--limit", type=int)

    # search
    p = sub.add_parser("search", help="Busca arquivos por nome")
    p.add_argument("pattern", help="Texto para buscar no caminho")
    p.add_argument("--regex", action="store_true", help="Usa regex em vez de LIKE (ex: '.*\\.raw$')")

    # duplicates
    p = sub.add_parser("duplicates", help="Encontra duplicatas por hash")
    p.add_argument("--across", action="store_true", help="Só mostra duplicatas entre snapshots diferentes")
    p.add_argument("--limit", type=int, help="Máximo de grupos a mostrar")

    # compare
    p = sub.add_parser("compare", help="Compara dois snapshots")
    p.add_argument("id1", type=int)
    p.add_argument("id2", type=int)

    # export
    p = sub.add_parser("export", help="Exporta snapshot")
    p.add_argument("snapshot_id", type=int)
    p.add_argument("--format", choices=["json", "csv"], default="json")

    # mount
    p = sub.add_parser("mount", help="Monta snapshot como pasta virtual (FUSE)")
    p.add_argument("snapshot_id", type=int)
    p.add_argument("mountpoint", help="Pasta onde montar (ex: /tmp/snapshot-hd)")

    # pending
    p = sub.add_parser("pending", help="Mostra operações pendentes")
    p.add_argument("snapshot_id", type=int, nargs="?")

    # apply
    p = sub.add_parser("apply", help="Aplica operações pendentes no drive real")
    p.add_argument("snapshot_id", type=int)
    p.add_argument("mount_path", help="Ponto de montagem REAL do drive")
    p.add_argument("--dry-run", action="store_true", help="Mostra o preview sem executar nada")

    # delete
    p = sub.add_parser("delete", help="Remove um snapshot")
    p.add_argument("snapshot_id", type=int)

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(0)

    # Inicializa logging antes de despachar qualquer comando
    _setup_logging(args.verbose)

    {
        "snapshot": cmd_snapshot,
        "list": cmd_list,
        "files": cmd_files,
        "search": cmd_search,
        "duplicates": cmd_duplicates,
        "compare": cmd_compare,
        "export": cmd_export,
        "mount": cmd_mount,
        "pending": cmd_pending,
        "apply": cmd_apply,
        "delete": cmd_delete,
    }[args.command](args)


if __name__ == "__main__":
    main()
