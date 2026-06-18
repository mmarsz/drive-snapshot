#!/usr/bin/env python3
"""Verifica duplicatas byte-a-byte (cmp) e remove APENAS as cópias 100% idênticas.

Reconstrói os grupos de arquivos grandes (>=20MB, dado real) do snapshot #9,
mantém a cópia de caminho mais curto e, para cada cópia redundante, faz um
filecmp.cmp(shallow=False) (comparação byte-a-byte real) contra a mantida.
Só remove se forem idênticas; qualquer divergência/erro é pulado e reportado.
"""
import filecmp
import os
import re
import sqlite3
from collections import defaultdict

DB = "/home/mmarsz/pessoal/drive-snapshot/snapshots.db"
SNAP = 9
MIN_FILE = 20 * 1024 * 1024
HOME = "/home/mmarsz/"
NOISE = re.compile(
    r"target/|\.arduino15/|\.platformio/|\.local/lib/|\.config/Code/|"
    r"squashfs-root/|\.cargo/|\.rustup/|site-packages/|\.cache/"
)


def h(n):
    n = float(n or 0)
    for u in "B KB MB GB TB".split():
        if abs(n) < 1024:
            return f"{n:.1f}{u}"
        n /= 1024
    return f"{n:.1f}PB"


def main():
    db = sqlite3.connect(DB)
    db.row_factory = sqlite3.Row
    rows = db.execute(
        """SELECT path,size,sha256 FROM files WHERE snapshot_id=? AND sha256 IN(
             SELECT sha256 FROM files WHERE snapshot_id=? AND sha256 IS NOT NULL
             GROUP BY sha256 HAVING COUNT(*)>1)""",
        (SNAP, SNAP),
    ).fetchall()
    groups = defaultdict(list)
    size = {}
    for r in rows:
        groups[r["sha256"]].append(r["path"])
        size[r["sha256"]] = r["size"]

    items = []
    for sha, paths in groups.items():
        if size[sha] >= MIN_FILE and not all(NOISE.search(p) for p in paths):
            items.append((size[sha], sorted(paths, key=lambda p: (len(p), p))))
    items.sort(reverse=True)

    freed = 0
    removed = confirmed = skipped = 0
    print(f"Verificando {len(items)} grupos de arquivos grandes (cmp byte-a-byte)...\n")
    for sz, paths in items:
        keep = HOME + paths[0]
        if not os.path.isfile(keep):
            print(f"  SKIP grupo (mantido sumiu): {paths[0]}")
            skipped += len(paths) - 1
            continue
        for p in paths[1:]:
            dele = HOME + p
            if not os.path.isfile(dele):
                print(f"  SKIP (já não existe): {p}")
                skipped += 1
                continue
            try:
                identical = filecmp.cmp(keep, dele, shallow=False)
            except OSError as e:
                print(f"  ERRO cmp ({e}): {p}")
                skipped += 1
                continue
            if identical:
                confirmed += 1
                try:
                    os.remove(dele)
                    removed += 1
                    freed += sz
                    print(f"  OK  ({h(sz)}) removido: {p}")
                except OSError as e:
                    print(f"  ERRO rm ({e}): {p}")
            else:
                print(f"  ⚠ DIFERENTE (NÃO removido): {p}  [hash igual mas cmp divergiu!]")
                skipped += 1

    print(
        f"\nResumo: {confirmed} confirmados idênticos, {removed} removidos, "
        f"{skipped} pulados. Espaço liberado: {h(freed)}"
    )


if __name__ == "__main__":
    main()
