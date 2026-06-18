#!/usr/bin/env python3
"""
drive-snapshot: Cataloga arquivos de HDs/SSDs que não podem rodar ao mesmo tempo.
Cria snapshots com hashes para encontrar duplicatas entre drives.

Uso:
  ./drive-snapshot.py snapshot <caminho> [--label NOME]   Escaneia um drive montado
                       [--quick] [--jobs N] [--exclude P]  (--quick=fingerprint, --jobs=threads, --exclude=glob)
  ./drive-snapshot.py update <snapshot_id> [caminho]       Snapshot incremental (reusa hashes)
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
import fnmatch
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
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
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
    # Espera até 30s por um lock em vez de falhar na hora — permite dois scans
    # (ex.: dois snapshots em paralelo) escrevendo no mesmo banco sem "database is locked".
    db.execute("PRAGMA busy_timeout=30000")
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
    db.execute("""
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_id INTEGER NOT NULL,
            path TEXT NOT NULL,
            size INTEGER NOT NULL,
            mtime REAL,
            sha256 TEXT,
            quick_hash TEXT,
            FOREIGN KEY (snapshot_id) REFERENCES snapshots(id) ON DELETE CASCADE
        )
    """)
    db.execute("CREATE INDEX IF NOT EXISTS idx_files_snapshot ON files(snapshot_id)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_files_hash ON files(sha256)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_files_path ON files(path)")
    # idx_files_quick NÃO é criado aqui: em bancos antigos a coluna quick_hash só
    # existe após _migrate_db(). O índice é criado lá (idempotente p/ bancos novos).
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


def _migrate_db(db):
    """Executa migrações de schema pendentes (chamado uma vez no startup)."""
    # Verifica se a coluna status existe em bancos criados antes desta versão
    cols = {row[1] for row in db.execute("PRAGMA table_info(snapshots)").fetchall()}
    if "status" not in cols:
        db.execute("ALTER TABLE snapshots ADD COLUMN status TEXT DEFAULT 'complete'")
        db.commit()
    # quick_hash: impressão digital barata para o modo --quick
    file_cols = {row[1] for row in db.execute("PRAGMA table_info(files)").fetchall()}
    if "quick_hash" not in file_cols:
        db.execute("ALTER TABLE files ADD COLUMN quick_hash TEXT")
        db.commit()
    # Índice criado aqui (não em get_db) pois depende da coluna já existir.
    # IF NOT EXISTS torna idempotente tanto p/ bancos novos quanto migrados.
    db.execute("CREATE INDEX IF NOT EXISTS idx_files_quick ON files(quick_hash)")
    db.commit()


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


def quick_fingerprint(filepath, size):
    """Impressão digital barata: tamanho + sha256 dos primeiros e últimos 64KB.

    Para arquivos grandes lê só 128KB em vez de gigabytes — muito mais rápido que
    o SHA256 completo. Colisões são raras mas possíveis (dois arquivos de mesmo
    tamanho e mesmas bordas, diferentes no meio); por isso é uma aproximação,
    adequada ao modo --quick. Retorna string 'size:hash16' ou None se inacessível.
    """
    h = hashlib.sha256()
    try:
        with open(filepath, "rb") as f:
            h.update(f.read(65536))
            if size > 131072:
                f.seek(-65536, os.SEEK_END)
                h.update(f.read(65536))
        return f"{size}:{h.hexdigest()[:16]}"
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


@dataclass
class _ScanState:
    """Estado mutável compartilhado durante o scan de arquivos."""
    total_size: int = 0
    hashed: int = 0
    reused: int = 0      # hashes reaproveitados de um snapshot anterior (update)
    skipped: int = 0
    batch: list = field(default_factory=list)
    interrupted: bool = False


def _detect_label(mount):
    """Auto-detecta o label do filesystem via lsblk/findmnt; fallback = basename."""
    try:
        result = subprocess.run(
            ['lsblk', '-no', 'LABEL', mount],
            capture_output=True, text=True, timeout=5,
        )
        detected = result.stdout.strip()
        if detected:
            return detected
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return os.path.basename(mount) or mount
    # lsblk falha em subdiretórios — resolve o mount point real com findmnt
    try:
        mount_result = subprocess.run(
            ['findmnt', '-no', 'TARGET', '--target', mount],
            capture_output=True, text=True, timeout=5,
        )
        mount_point = mount_result.stdout.strip() or mount
        result = subprocess.run(
            ['lsblk', '-no', 'LABEL', mount_point],
            capture_output=True, text=True, timeout=5,
        )
        detected = result.stdout.strip()
        if detected:
            return detected
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return os.path.basename(mount) or mount


def _detect_jobs(mount, requested):
    """Decide quantas threads usar para hashing.

    requested > 0 → respeita. requested <= 0 (auto) → detecta se o dispositivo é
    rotacional (HD mecânico) via `lsblk -no ROTA`. HD → 1 thread (leitura paralela
    causa thrashing de seek). SSD/NVMe → paraleliza até min(8, CPUs).
    Em caso de dúvida, assume HD (serial) por segurança.
    """
    if requested and requested > 0:
        return requested
    rotational = True  # default seguro: serial
    try:
        r = subprocess.run(
            ['lsblk', '-no', 'ROTA', '--target', mount],
            capture_output=True, text=True, timeout=5,
        )
        vals = [v.strip() for v in r.stdout.split() if v.strip()]
        if vals:
            rotational = any(v == '1' for v in vals)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    if rotational:
        return 1
    return min(8, os.cpu_count() or 2)


def _scan_into_snapshot(args, mount, label, prior=None, new_snapshot=False):
    """Núcleo de scan compartilhado por `snapshot` e `update`.

    prior: dict opcional {relpath: (size, mtime, sha256)} de um snapshot anterior;
           arquivos com mesmo size+mtime reaproveitam o hash (sem reler o arquivo).
    new_snapshot: se True, sempre cria um snapshot novo (não tenta retomar) —
           usado por `update` para preservar o histórico.
    """
    logger = logging.getLogger("drive-snapshot")
    quick = getattr(args, "quick", False)
    no_hash = getattr(args, "no_hash", False)
    jobs = _detect_jobs(mount, getattr(args, "jobs", 0))
    exclude = getattr(args, "exclude", None) or []

    def _excluded(name, rel):
        """True se o nome (basename) ou o caminho relativo casar com algum padrão."""
        return any(
            fnmatch.fnmatch(name, pat) or fnmatch.fnmatch(rel, pat)
            for pat in exclude
        )

    def walk_error(err):
        print(f"\n  AVISO: não foi possível acessar: {err.filename} ({err})", file=sys.stderr)
        logger.warning(f"Erro de acesso durante walk: {err.filename} ({err})")

    print("Contando arquivos...", end="", flush=True)
    file_list = []
    for root, dirs, filenames in os.walk(mount, onerror=walk_error):
        rel_root = os.path.relpath(root, mount)
        rel_root = "" if rel_root == "." else rel_root
        # Poda diretórios excluídos in-place: os.walk não desce neles (mais rápido)
        if exclude:
            dirs[:] = [
                d for d in dirs
                if not _excluded(d, os.path.join(rel_root, d) if rel_root else d)
            ]
        for fname in filenames:
            rel = os.path.join(rel_root, fname) if rel_root else fname
            if exclude and _excluded(fname, rel):
                continue
            fpath = os.path.join(root, fname)
            if os.path.isfile(fpath) and not os.path.islink(fpath):
                file_list.append(fpath)
    total = len(file_list)
    mode_desc = "quick" if quick else ("sem hash" if no_hash else "sha256")
    print(f" {total} arquivos encontrados. (modo: {mode_desc}, jobs: {jobs})")

    if total == 0:
        print("Nenhum arquivo encontrado. Nada a fazer.")
        logger.info("Nenhum arquivo encontrado. Snapshot cancelado.")
        return

    db = get_db()

    # Retoma snapshot interrompido com mesmo label+mount (exceto em update)
    already_scanned = set()
    initial_size = 0
    initial_hashed = 0
    existing = None
    if not new_snapshot:
        existing = db.execute(
            "SELECT id FROM snapshots WHERE label = ? AND mount_path = ? AND status IN ('scanning', 'interrupted')",
            (label, mount),
        ).fetchone()
    if existing:
        snap_id = existing["id"]
        already_scanned = {
            r["path"] for r in db.execute(
                "SELECT path FROM files WHERE snapshot_id = ?", (snap_id,)
            ).fetchall()
        }
        stats = db.execute(
            "SELECT COUNT(*) as cnt, COALESCE(SUM(size), 0) as total FROM files WHERE snapshot_id = ?",
            (snap_id,),
        ).fetchone()
        initial_size = stats["total"]
        initial_hashed = db.execute(
            "SELECT COUNT(*) as cnt FROM files WHERE snapshot_id = ? AND sha256 IS NOT NULL",
            (snap_id,),
        ).fetchone()["cnt"]
        print(f"  Retomando snapshot #{snap_id} ({len(already_scanned)} arquivos já escaneados)")
        logger.info(f"Retomando snapshot #{snap_id} com {len(already_scanned)} arquivos já escaneados")
    else:
        cur = db.execute(
            "INSERT INTO snapshots (label, mount_path, created_at, status) VALUES (?, ?, ?, 'scanning')",
            (label, mount, datetime.now().isoformat()),
        )
        snap_id = cur.lastrowid
        db.commit()

    # Pré-filtra arquivos já escaneados (resume) — barato e melhora o paralelismo
    if already_scanned:
        pending = [f for f in file_list if os.path.relpath(f, mount) not in already_scanned]
    else:
        pending = file_list
    n_pending = len(pending)

    batch_size = 500
    start = time.time()
    state = _ScanState(total_size=initial_size, hashed=initial_hashed)

    def _handle_sigint(signum, frame):
        state.interrupted = True

    old_handler = signal.signal(signal.SIGINT, _handle_sigint)

    def _scan_one(fpath):
        """Worker puro (sem DB): stat + hash/fingerprint. Roda em threads."""
        try:
            relpath = os.path.relpath(fpath, mount)
            st = os.stat(fpath)
            size, mtime = st.st_size, st.st_mtime
            sha = q = None
            reused = False
            # prior = {relpath: (size, mtime, sha256, quick_hash)} de um snapshot anterior
            old = prior.get(relpath) if prior is not None else None
            unchanged = bool(old and old[0] == size and old[1] == mtime)
            if quick:
                if unchanged and old[3]:
                    q, reused = old[3], True
                else:
                    q = quick_fingerprint(fpath, size)
            elif not no_hash:
                if unchanged and old[2]:
                    sha, reused = old[2], True
                else:
                    sha = hash_file(fpath)
            return ("row", relpath, size, mtime, sha, q, reused)
        except (UnicodeDecodeError, OSError) as e:
            return ("err", fpath, e)

    def _consume(result):
        kind = result[0]
        if kind == "err":
            logger.debug(f"Arquivo ignorado (inacessível): {result[1]} — {result[2]}")
            state.skipped += 1
            return
        _, relpath, size, mtime, sha, q, reused = result
        state.total_size += size
        if reused:
            state.reused += 1
        elif sha:
            state.hashed += 1
        state.batch.append((snap_id, relpath, size, mtime, sha, q))
        if len(state.batch) >= batch_size:
            db.executemany(
                "INSERT INTO files (snapshot_id, path, size, mtime, sha256, quick_hash) VALUES (?, ?, ?, ?, ?, ?)",
                state.batch,
            )
            db.commit()
            state.batch.clear()

    def _results():
        """Itera resultados; serial (jobs<=1) ou via thread pool com janela limitada.

        Mantém no máximo 2*jobs hashes em voo em vez de submeter o drive inteiro de
        uma vez. Isso limita quanto trabalho continua rodando após um Ctrl+C: ao sair
        do `with`, o shutdown(wait=True) espera só esses poucos futures, então a
        interrupção é rápida em vez de aguardar milhares de hashes já submetidos
        (threads do pool não são daemon e bloqueiam o sys.exit).
        """
        if jobs <= 1:
            for f in pending:
                if state.interrupted:
                    return
                yield _scan_one(f)
            return
        with ThreadPoolExecutor(max_workers=jobs) as ex:
            src = iter(pending)
            inflight = deque()
            for f in src:
                inflight.append(ex.submit(_scan_one, f))
                if len(inflight) >= jobs * 2:
                    break
            while inflight:
                yield inflight.popleft().result()
                if state.interrupted:
                    break
                nxt = next(src, None)
                if nxt is not None:
                    inflight.append(ex.submit(_scan_one, nxt))

    def _run_scan_loop():
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
                task = progress.add_task("Escaneando", total=n_pending, size="0 B")
                for res in _results():
                    if state.interrupted:
                        break
                    _consume(res)
                    progress.update(task, advance=1, size=fmt_size(state.total_size))
        else:
            for i, res in enumerate(_results(), 1):
                if state.interrupted:
                    break
                _consume(res)
                if i % 100 == 0 or i == n_pending:
                    elapsed = time.time() - start
                    rate = i / elapsed if elapsed > 0 else 0
                    eta = (n_pending - i) / rate if rate > 0 else 0
                    print(
                        f"\r  [{i}/{n_pending}] {i*100//n_pending}% | "
                        f"{fmt_size(state.total_size)} | "
                        f"{rate:.0f} arq/s | "
                        f"ETA: {eta:.0f}s   ",
                        end="",
                        flush=True,
                    )

    _run_scan_loop()

    signal.signal(signal.SIGINT, old_handler)

    if state.batch:
        db.executemany(
            "INSERT INTO files (snapshot_id, path, size, mtime, sha256, quick_hash) VALUES (?, ?, ?, ?, ?, ?)",
            state.batch,
        )

    files_done = total - state.skipped
    if state.interrupted:
        db.execute(
            "UPDATE snapshots SET total_files = ?, total_size = ?, status = 'interrupted' WHERE id = ?",
            (files_done, state.total_size, snap_id),
        )
        db.commit()
        logger.info(f"Snapshot #{snap_id} interrompido pelo usuário. Progresso salvo.")
        print(f"\n\nInterrompido! Snapshot #{snap_id} salvo como 'interrompido'. Use 'snapshot' novamente para retomar.")
        sys.exit(130)

    db.execute(
        "UPDATE snapshots SET total_files = ?, total_size = ?, status = 'complete' WHERE id = ?",
        (files_done, state.total_size, snap_id),
    )
    db.commit()

    elapsed = time.time() - start
    print(f"\n\nSnapshot #{snap_id} criado!")
    print(f"  Arquivos: {files_done} ({state.skipped} inacessíveis)")
    print(f"  Tamanho total: {fmt_size(state.total_size)}")
    if quick:
        print(f"  Fingerprints (quick): {files_done}")
    else:
        print(f"  Hasheados: {state.hashed}" + (f" (+{state.reused} reaproveitados)" if state.reused else ""))
    print(f"  Tempo: {elapsed:.1f}s")
    logger.info(
        f"Snapshot #{snap_id} concluído: {files_done} arquivos, {fmt_size(state.total_size)}, "
        f"{state.skipped} inacessíveis, {state.hashed} hash, {state.reused} reaproveitados, {elapsed:.1f}s"
    )
    return snap_id


def cmd_snapshot(args):
    logger = logging.getLogger("drive-snapshot")
    mount = os.path.abspath(args.path)
    if not os.path.isdir(mount):
        print(f"Erro: '{mount}' não é um diretório válido.", file=sys.stderr)
        sys.exit(1)

    label = args.label or _detect_label(mount)
    print(f"Escaneando: {mount}")
    print(f"Label: {label}")
    logger.info(f"Iniciando snapshot: path={mount} label={label}")
    _scan_into_snapshot(args, mount, label)


def cmd_update(args):
    """Snapshot incremental: reaproveita hashes de um snapshot anterior para os
    arquivos inalterados (mesmo path+size+mtime), re-hasheando só o que mudou."""
    logger = logging.getLogger("drive-snapshot")
    db = get_db()
    base = db.execute("SELECT * FROM snapshots WHERE id = ?", (args.snapshot_id,)).fetchone()
    if not base:
        print(f"Snapshot #{args.snapshot_id} não encontrado.", file=sys.stderr)
        sys.exit(1)

    mount = os.path.abspath(args.path) if args.path else base["mount_path"]
    if not os.path.isdir(mount):
        print(f"Erro: '{mount}' não é um diretório válido (drive desconectado?).", file=sys.stderr)
        sys.exit(1)

    label = args.label or base["label"]

    # Carrega o snapshot-base como mapa relpath -> (size, mtime, sha256, quick_hash)
    prior = {
        r["path"]: (r["size"], r["mtime"], r["sha256"], r["quick_hash"])
        for r in db.execute(
            "SELECT path, size, mtime, sha256, quick_hash FROM files WHERE snapshot_id = ?",
            (args.snapshot_id,),
        ).fetchall()
    }
    print(f"Atualizando a partir do snapshot #{base['id']} '{base['label']}' ({len(prior)} arquivos base)")
    print(f"Escaneando: {mount}")
    print(f"Label: {label}")
    logger.info(f"Iniciando update: base=#{base['id']} path={mount} label={label} base_files={len(prior)}")
    _scan_into_snapshot(args, mount, label, prior=prior, new_snapshot=True)


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

        # Indicador de status: apenas exibido quando diferente de 'complete'
        status = r['status'] if r['status'] else 'complete'
        if status == 'scanning':
            status_tag = f" {_COLORS['yellow']}[SCANNING]{reset}{color}"
        elif status == 'interrupted':
            status_tag = f" {_COLORS['red']}[INTERRUPTED]{reset}{color}"
        else:
            status_tag = ""  # completo — sem indicador extra

        print(
            f"{color}{r['id']:>4}  {r['label']:<20}{status_tag}  {r['total_files']:>10}  "
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

    # Fix 8A: cláusula HAVING dinâmica elimina duplicação das duas queries
    having = "HAVING COUNT(DISTINCT snapshot_id) > 1" if args.across else "HAVING COUNT(*) > 1"
    desc = "entre drives diferentes" if args.across else "em todos os snapshots"

    # Chave de conteúdo: sha256 quando disponível, senão a fingerprint do modo --quick.
    # Os dois nunca colidem entre si (hex de 64 chars vs 'size:hash16').
    base_query = f"""
        SELECT COALESCE(sha256, quick_hash) as ck, size, COUNT(*) as cnt,
               COUNT(DISTINCT snapshot_id) as snap_cnt
        FROM files
        WHERE COALESCE(sha256, quick_hash) IS NOT NULL
        GROUP BY ck
        {having}
        ORDER BY size DESC
    """

    limit = args.limit or 50

    # Fix 10A: calcula totais via subquery — evita carregar todas as linhas em memória
    totals_query = f"""
        SELECT COUNT(*) as groups,
               SUM(size * (cnt - 1)) as wasted,
               SUM(cnt - 1) as copies
        FROM ({base_query})
    """
    totals = db.execute(totals_query).fetchone()

    if not totals["groups"]:
        print(f"Nenhuma duplicata encontrada {desc}.")
        return

    print(
        f"Duplicatas {desc}: {totals['groups']} grupos, "
        f"{totals['copies']} cópias extras ({fmt_size(totals['wasted'])} desperdiçado)\n"
    )

    # Busca somente as linhas necessárias para exibição (LIMIT aplicado no SQL)
    display_query = f"{base_query} LIMIT ?"
    display_rows = db.execute(display_query, (limit,)).fetchall()

    # Busca todas as localizações de uma vez (evita N+1 queries)
    all_hashes = [r["ck"] for r in display_rows]
    locs_by_hash = defaultdict(list)
    if all_hashes:
        placeholders = ",".join("?" * len(all_hashes))
        locations = db.execute(f"""
            SELECT COALESCE(f.sha256, f.quick_hash) as ck, f.path, s.label, s.id as snap_id
            FROM files f JOIN snapshots s ON f.snapshot_id = s.id
            WHERE COALESCE(f.sha256, f.quick_hash) IN ({placeholders})
            ORDER BY ck, s.label, f.path
        """, all_hashes).fetchall()
        for loc in locations:
            locs_by_hash[loc["ck"]].append(loc)

    shown = 0
    for r in display_rows:
        print(f"  Hash: {r['ck'][:16]}…  Tamanho: {fmt_size(r['size'])}  Cópias: {r['cnt']} (em {r['snap_cnt']} snapshot(s))")
        for loc in locs_by_hash[r["ck"]]:
            print(f"    [{loc['label']}#{loc['snap_id']}] {loc['path']}")
        print()
        shown += 1

    if totals["groups"] > limit:
        print(f"  ... e mais {totals['groups'] - limit} grupos. Use --limit para ver mais.")


def cmd_compare(args):
    db = get_db()
    s1 = db.execute("SELECT * FROM snapshots WHERE id = ?", (args.id1,)).fetchone()
    s2 = db.execute("SELECT * FROM snapshots WHERE id = ?", (args.id2,)).fetchone()

    if not s1 or not s2:
        print("Snapshot não encontrado.", file=sys.stderr)
        sys.exit(1)

    print(f"Comparando: #{s1['id']} {s1['label']}  vs  #{s2['id']} {s2['label']}\n")

    # Arquivos por chave de conteúdo (sha256 ou, no modo --quick, quick_hash)
    def get_hashes(snap_id):
        rows = db.execute(
            "SELECT COALESCE(sha256, quick_hash) as ck, path, size FROM files "
            "WHERE snapshot_id = ? AND COALESCE(sha256, quick_hash) IS NOT NULL",
            (snap_id,),
        ).fetchall()
        by_hash = {}
        for r in rows:
            by_hash.setdefault(r["ck"], []).append(r)
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
    changed = [(p, paths1[p], paths2[p]) for p in common_paths if paths1[p]["ck"] != paths2[p]["ck"]]

    if changed:
        print(f"\n  Mesmo caminho, conteúdo diferente: {len(changed)}")
        for p, f1, f2 in changed[:20]:
            print(f"    {p}")
            print(f"      #{s1['id']}: {fmt_size(f1['size'])}  {f1['ck'][:12]}…")
            print(f"      #{s2['id']}: {fmt_size(f2['size'])}  {f2['ck'][:12]}…")
        if len(changed) > 20:
            print(f"    ... e mais {len(changed) - 20}")


def cmd_export(args):
    db = get_db()
    snap = db.execute("SELECT * FROM snapshots WHERE id = ?", (args.snapshot_id,)).fetchone()
    if not snap:
        print(f"Snapshot #{args.snapshot_id} não encontrado.", file=sys.stderr)
        sys.exit(1)

    rows = db.execute(
        "SELECT path, size, mtime, sha256, quick_hash FROM files WHERE snapshot_id = ? ORDER BY path",
        (args.snapshot_id,),
    ).fetchall()

    if args.format == "csv":
        import csv
        outfile = f"snapshot_{args.snapshot_id}_{snap['label']}.csv"
        with open(outfile, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["path", "size", "mtime", "sha256", "quick_hash"])
            for r in rows:
                w.writerow([r["path"], r["size"], r["mtime"], r["sha256"], r["quick_hash"]])
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

    # Desempate por profundidade: rmdir do mais fundo pro mais raso (senão o pai
    # é removido antes do filho e dá ENOTEMPTY); mkdir do mais raso pro mais fundo.
    _PRIO = {"mkdir": 1, "move": 2, "delete": 3, "rmdir": 4}

    def _depth_key(r):
        depth = (r["src_path"] or "").count("/")
        if r["op_type"] == "rmdir":
            depth = -depth  # mais profundo primeiro
        return (_PRIO.get(r["op_type"], 5), depth, r["created_at"])

    rows = sorted(rows, key=_depth_key)

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
    applied_ids = []  # só operações concluídas são removidas de pending_ops
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
            applied_ids.append(r["id"])
        except Exception as e:
            print(f"  ERRO: {op_type} {src_path}: {e}")
            logger.warning(f"APPLY #{snap_id}: {op_type} {src_path} FAILED: {e}")
            erros += 1

    # Limpa SOMENTE as operações concluídas com sucesso. Conflitos e erros
    # permanecem em pending_ops para que possam ser revistos/reaplicados depois.
    if applied_ids:
        placeholders = ",".join("?" * len(applied_ids))
        db.execute(
            f"DELETE FROM pending_ops WHERE id IN ({placeholders})", applied_ids
        )
        db.commit()

    restantes = len(rows) - len(applied_ids)
    print(f"\nConcluído: {ok} ok, {erros} erros, {conflitos} conflitos.")
    if restantes:
        print(f"  {restantes} operação(ões) mantida(s) como pendente(s) para retry.")
    logger.info(
        f"APPLY #{snap_id}: concluído - {ok} ok, {erros} erros, "
        f"{conflitos} conflitos, {restantes} mantidas pendentes"
    )


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
    p.add_argument("--quick", action="store_true",
                   help="Fingerprint barata (size + bordas de 64KB) em vez de SHA256 completo — muito mais rápido, duplicatas aproximadas")
    p.add_argument("--jobs", "-j", type=int, default=0,
                   help="Threads de hashing (0=auto: 1 em HD mecânico, N em SSD/NVMe)")
    p.add_argument("--exclude", action="append", metavar="PADRÃO",
                   help="Ignora arquivos/dirs que casem com o padrão glob (repetível). Ex: --exclude node_modules --exclude '.cache' --exclude '*.pyc'")

    # update
    p = sub.add_parser("update", help="Snapshot incremental (reaproveita hashes inalterados)")
    p.add_argument("snapshot_id", type=int, help="Snapshot-base a partir do qual reaproveitar hashes")
    p.add_argument("path", nargs="?", help="Caminho do drive (default: mount_path do snapshot-base)")
    p.add_argument("--label", "-l", help="Label do novo snapshot (default: o do snapshot-base)")
    p.add_argument("--no-hash", action="store_true", help="Pula o cálculo de SHA256")
    p.add_argument("--quick", action="store_true", help="Fingerprint barata em vez de SHA256 completo")
    p.add_argument("--jobs", "-j", type=int, default=0, help="Threads de hashing (0=auto)")
    p.add_argument("--exclude", action="append", metavar="PADRÃO",
                   help="Ignora arquivos/dirs que casem com o padrão glob (repetível)")

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

    # Executa migrações de schema uma única vez por invocação
    _mig_db = get_db()
    _migrate_db(_mig_db)
    _mig_db.close()

    {
        "snapshot": cmd_snapshot,
        "update": cmd_update,
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
