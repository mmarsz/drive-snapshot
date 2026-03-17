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
import os
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path

DB_PATH = Path(__file__).parent / "snapshots.db"

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
            total_size INTEGER DEFAULT 0
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
    mount = os.path.abspath(args.path)
    if not os.path.isdir(mount):
        print(f"Erro: '{mount}' não é um diretório válido.", file=sys.stderr)
        sys.exit(1)

    label = args.label or os.path.basename(mount) or mount
    print(f"Escaneando: {mount}")
    print(f"Label: {label}")

    # Callback para erros de acesso durante os.walk (ex.: nomes não-UTF-8 em NTFS/exFAT)
    def walk_error(err):
        print(f"\n  AVISO: não foi possível acessar: {err.filename} ({err})", file=sys.stderr)

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
        return

    db = get_db()
    cur = db.execute(
        "INSERT INTO snapshots (label, mount_path, created_at) VALUES (?, ?, ?)",
        (label, mount, datetime.now().isoformat()),
    )
    snap_id = cur.lastrowid

    total_size = 0
    hashed = 0
    skipped = 0
    batch = []
    batch_size = 500
    start = time.time()

    for i, fpath in enumerate(file_list, 1):
        try:
            stat = os.stat(fpath)
            size = stat.st_size
            mtime = stat.st_mtime

            # Path relativo ao mount point
            relpath = os.path.relpath(fpath, mount)

            # Hash (pula arquivos enormes >4GB com flag, mas faz por padrão)
            if args.no_hash:
                sha = None
            else:
                sha = hash_file(fpath)
                if sha:
                    hashed += 1

            total_size += size
            batch.append((snap_id, relpath, size, mtime, sha))
        except (UnicodeDecodeError, OSError):
            # Ignora arquivos com nomes/conteúdo não-decodificável ou inacessíveis
            skipped += 1
            continue

        if len(batch) >= batch_size:
            db.executemany(
                "INSERT INTO files (snapshot_id, path, size, mtime, sha256) VALUES (?, ?, ?, ?, ?)",
                batch,
            )
            db.commit()
            batch.clear()

        # Progresso
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

    # Resto do batch
    if batch:
        db.executemany(
            "INSERT INTO files (snapshot_id, path, size, mtime, sha256) VALUES (?, ?, ?, ?, ?)",
            batch,
        )

    db.execute(
        "UPDATE snapshots SET total_files = ?, total_size = ? WHERE id = ?",
        (total - skipped, total_size, snap_id),
    )
    db.commit()

    elapsed = time.time() - start
    print(f"\n\nSnapshot #{snap_id} criado!")
    print(f"  Arquivos: {total - skipped} ({skipped} inacessíveis)")
    print(f"  Tamanho total: {fmt_size(total_size)}")
    print(f"  Hasheados: {hashed}")
    print(f"  Tempo: {elapsed:.1f}s")


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
        print(
            f"{r['id']:>4}  {r['label']:<20}  {r['total_files']:>10}  "
            f"{fmt_size(r['total_size']):>10}  {r['created_at'][:19]:>20}  {r['mount_path']}"
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

    if not rows:
        print(f"Nenhum arquivo encontrado com '{args.pattern}'.")
        return

    print(f"Resultados para '{args.pattern}' ({len(rows)} encontrados):\n")
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
    print(f"Duplicatas {desc}: {len(rows)} grupos ({fmt_size(total_wasted)} desperdiçado em cópias extras)\n")

    shown = 0
    for r in rows[:limit]:
        print(f"  Hash: {r['sha256'][:16]}…  Tamanho: {fmt_size(r['size'])}  Cópias: {r['cnt']} (em {r['snap_cnt']} snapshot(s))")

        # Mostra onde estão
        locs = db.execute(
            """
            SELECT f.path, s.label, s.id as snap_id
            FROM files f JOIN snapshots s ON f.snapshot_id = s.id
            WHERE f.sha256 = ?
            """,
            (r["sha256"],),
        ).fetchall()
        for loc in locs:
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

    FUSE(SnapshotFS(args.snapshot_id), mountpoint, foreground=True, allow_other=False, nothreads=False)


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
    db = get_db()
    snap = db.execute("SELECT * FROM snapshots WHERE id = ?", (args.snapshot_id,)).fetchone()
    if not snap:
        print(f"Snapshot #{args.snapshot_id} não encontrado.", file=sys.stderr)
        sys.exit(1)

    mount_real = os.path.abspath(args.mount_path)
    if not os.path.isdir(mount_real):
        print(f"Erro: '{mount_real}' não é um diretório válido.", file=sys.stderr)
        sys.exit(1)

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
        try:
            if r["op_type"] == "mkdir":
                # Valida src_path antes de criar diretório
                try:
                    target = _validate_path(mount_real, r["src_path"])
                except ValueError:
                    print(f"ERRO: path inválido (escapa mount): {r['src_path']}")
                    erros += 1
                    continue
                os.makedirs(target, exist_ok=True)
                print(f"  OK mkdir: {r['src_path']}")
            elif r["op_type"] == "move":
                # Valida src_path e dst_path antes de mover
                try:
                    src = _validate_path(mount_real, r["src_path"])
                    dst = _validate_path(mount_real, r["dst_path"])
                except ValueError:
                    # Reporta qual dos dois caminhos disparou a violação
                    print(f"ERRO: path inválido (escapa mount): {r['src_path']} → {r['dst_path']}")
                    erros += 1
                    continue
                # Proteção contra sobrescrita silenciosa: se o destino já existe,
                # aborta a operação e registra como conflito
                if os.path.exists(dst):
                    print(f"  CONFLITO: destino já existe: {r['dst_path']} (pulando)")
                    conflitos += 1
                    continue
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                shutil.move(src, dst)
                print(f"  OK move: {r['src_path']} → {r['dst_path']}")
            elif r["op_type"] == "delete":
                # Valida src_path antes de deletar arquivo
                try:
                    target = _validate_path(mount_real, r["src_path"])
                except ValueError:
                    print(f"ERRO: path inválido (escapa mount): {r['src_path']}")
                    erros += 1
                    continue
                if os.path.isfile(target):
                    os.remove(target)
                    print(f"  OK delete: {r['src_path']}")
                else:
                    print(f"  SKIP (não encontrado): {r['src_path']}")
            elif r["op_type"] == "rmdir":
                # Valida src_path antes de remover diretório
                try:
                    target = _validate_path(mount_real, r["src_path"])
                except ValueError:
                    print(f"ERRO: path inválido (escapa mount): {r['src_path']}")
                    erros += 1
                    continue
                if os.path.isdir(target):
                    os.rmdir(target)
                    print(f"  OK rmdir: {r['src_path']}")
                else:
                    print(f"  SKIP (não encontrado): {r['src_path']}")
            ok += 1
        except Exception as e:
            print(f"  ERRO: {r['op_type']} {r['src_path']}: {e}")
            erros += 1

    # Limpa operações aplicadas
    db.execute("DELETE FROM pending_ops WHERE snapshot_id = ?", (args.snapshot_id,))
    db.commit()

    print(f"\nConcluído: {ok} ok, {erros} erros, {conflitos} conflitos.")


def cmd_delete(args):
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


# --- CLI ---

def main():
    parser = argparse.ArgumentParser(
        description="Cataloga arquivos de drives para comparação offline.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
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
