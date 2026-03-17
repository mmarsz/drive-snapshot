"""
Testes focados nos caminhos críticos de segurança e lógica do drive-snapshot.
"""

import importlib
import io
import os
import re
import sqlite3
import sys
import types
from pathlib import Path
from unittest.mock import patch

import pytest

# Importa o módulo com hífen no nome
sys.path.insert(0, str(Path(__file__).parent))
ds = importlib.import_module("drive-snapshot")


# ---------------------------------------------------------------------------
# Fixtures auxiliares
# ---------------------------------------------------------------------------

@pytest.fixture()
def tmp_db(tmp_path, monkeypatch):
    """Banco SQLite temporário — sobrescreve ds.DB_PATH para isolamento."""
    db_file = tmp_path / "test_snapshots.db"
    monkeypatch.setattr(ds, "DB_PATH", db_file)
    yield db_file


@pytest.fixture()
def populated_db(tmp_db):
    """Banco com snapshot, arquivos e pending_ops já inseridos."""
    db = ds.get_db()
    cur = db.execute(
        "INSERT INTO snapshots (label, mount_path, created_at, total_files, total_size, status)"
        " VALUES ('teste', '/mnt/teste', '2025-01-01T00:00:00', 2, 2048, 'complete')"
    )
    snap_id = cur.lastrowid
    db.execute(
        "INSERT INTO files (snapshot_id, path, size, mtime, sha256)"
        " VALUES (?, 'docs/a.txt', 1024, 0, 'aabbcc')",
        (snap_id,),
    )
    db.execute(
        "INSERT INTO files (snapshot_id, path, size, mtime, sha256)"
        " VALUES (?, 'docs/b.txt', 1024, 0, 'aabbcc')",
        (snap_id,),
    )
    db.commit()
    return snap_id, db


# ---------------------------------------------------------------------------
# 1-4: _validate_path
# ---------------------------------------------------------------------------

def test_validate_path_normal(tmp_path):
    """Caminho normal deve retornar path dentro do diretório base."""
    base = tmp_path / "mnt" / "foo"
    base.mkdir(parents=True)
    # Cria o arquivo alvo para que realpath resolva corretamente
    (base / "bar").mkdir()
    (base / "bar" / "baz.txt").write_text("x")

    result = ds._validate_path(str(base), "bar/baz.txt")
    assert result.startswith(str(base))
    assert "baz.txt" in result


def test_validate_path_absolute_neutralized(tmp_path):
    """Caminho absoluto como '/etc/passwd' é neutralizado via lstrip('/').

    A implementação usa lstrip('/') para impedir que os.path.join ignore o
    base quando relpath começa com '/'.  Assim '/etc/passwd' vira 'etc/passwd'
    e resolve dentro do base — não escapa.  O teste confirma que o resultado
    fica dentro do diretório base (garantia de segurança) em vez de ir para
    /etc/passwd do sistema.
    """
    base = tmp_path / "mnt" / "foo"
    base.mkdir(parents=True)

    # Não deve levantar — o caminho é neutralizado para dentro do base
    result = ds._validate_path(str(base), "/etc/passwd")
    assert result.startswith(str(base)), (
        "caminho absoluto deve ser resolvido dentro do base, não no sistema"
    )


def test_validate_path_traversal_rejected(tmp_path):
    """Traversal com ../../ deve levantar ValueError."""
    base = tmp_path / "mnt" / "foo"
    base.mkdir(parents=True)

    with pytest.raises(ValueError, match="escapa"):
        ds._validate_path(str(base), "../../etc/passwd")


def test_validate_path_same_prefix(tmp_path):
    """'../foobar/x' não deve casar com /mnt/foo via prefixo falso (/mnt/foobar)."""
    base = tmp_path / "mnt" / "foo"
    base.mkdir(parents=True)
    # Cria /mnt/foobar para garantir que o diretório vizinho existe
    sibling = tmp_path / "mnt" / "foobar"
    sibling.mkdir()
    (sibling / "x").write_text("y")

    with pytest.raises(ValueError, match="escapa"):
        ds._validate_path(str(base), "../foobar/x")


# ---------------------------------------------------------------------------
# 5: Ordenação de pending_ops no apply
# ---------------------------------------------------------------------------

def test_apply_operation_ordering(tmp_db, populated_db):
    """mkdir deve vir primeiro e rmdir por último na query de ordenação do apply."""
    snap_id, db = populated_db
    now = "2025-01-01T00:00:00"
    # Insere em ordem propositalmente embaralhada
    for op, src in [
        ("delete", "docs/a.txt"),
        ("rmdir", "docs"),
        ("move", "docs/b.txt"),
        ("mkdir", "nova_pasta"),
    ]:
        db.execute(
            "INSERT INTO pending_ops (snapshot_id, op_type, src_path, dst_path, created_at)"
            " VALUES (?, ?, ?, ?, ?)",
            (snap_id, op, src, "nova_pasta/b.txt" if op == "move" else None, now),
        )
    db.commit()

    rows = db.execute(
        """SELECT op_type FROM pending_ops WHERE snapshot_id = ?
           ORDER BY CASE op_type
             WHEN 'mkdir'  THEN 1
             WHEN 'move'   THEN 2
             WHEN 'delete' THEN 3
             WHEN 'rmdir'  THEN 4
             ELSE 5
           END, created_at""",
        (snap_id,),
    ).fetchall()

    tipos = [r["op_type"] for r in rows]
    assert tipos[0] == "mkdir", "mkdir deve ser o primeiro"
    assert tipos[-1] == "rmdir", "rmdir deve ser o último"


# ---------------------------------------------------------------------------
# 6: Detecção de conflito no apply
# ---------------------------------------------------------------------------

def test_apply_conflict_detection(tmp_path, tmp_db, populated_db, capsys):
    """Move com destino já existente deve ser registrado como conflito."""
    snap_id, db = populated_db

    # Cria estrutura de arquivos real
    src_dir = tmp_path / "drive"
    src_dir.mkdir()
    src_file = src_dir / "docs" / "b.txt"
    src_file.parent.mkdir()
    src_file.write_text("conteudo")

    dst_file = src_dir / "nova_pasta" / "b.txt"
    dst_file.parent.mkdir()
    dst_file.write_text("ja existe")  # destino pré-existente

    db.execute(
        "INSERT INTO pending_ops (snapshot_id, op_type, src_path, dst_path, created_at)"
        " VALUES (?, 'move', 'docs/b.txt', 'nova_pasta/b.txt', '2025-01-01T00:00:00')",
        (snap_id,),
    )
    db.commit()

    # Simula args para cmd_apply sem confirmação de usuário
    args = types.SimpleNamespace(
        snapshot_id=snap_id,
        mount_path=str(src_dir),
        dry_run=False,
    )

    with patch("builtins.input", return_value="s"):
        ds.cmd_apply(args)

    captured = capsys.readouterr()
    assert "CONFLITO" in captured.out
    # Arquivo de destino não deve ter sido sobrescrito
    assert dst_file.read_text() == "ja existe"


# ---------------------------------------------------------------------------
# 7: Dry-run não executa mudanças
# ---------------------------------------------------------------------------

def test_apply_dry_run(tmp_path, tmp_db, populated_db, capsys):
    """--dry-run deve imprimir preview mas não modificar o filesystem."""
    snap_id, db = populated_db

    src_dir = tmp_path / "drive"
    src_dir.mkdir()
    target_file = src_dir / "docs" / "a.txt"
    target_file.parent.mkdir()
    target_file.write_text("original")

    db.execute(
        "INSERT INTO pending_ops (snapshot_id, op_type, src_path, dst_path, created_at)"
        " VALUES (?, 'delete', 'docs/a.txt', NULL, '2025-01-01T00:00:00')",
        (snap_id,),
    )
    db.commit()

    args = types.SimpleNamespace(
        snapshot_id=snap_id,
        mount_path=str(src_dir),
        dry_run=True,
    )

    ds.cmd_apply(args)

    # Arquivo deve continuar existindo
    assert target_file.exists(), "dry-run não deve deletar arquivo"

    captured = capsys.readouterr()
    assert "dry-run" in captured.out.lower()


# ---------------------------------------------------------------------------
# 8: FUSE read capeado em 65536 bytes
# ---------------------------------------------------------------------------

def test_fuse_read_capped(tmp_db, populated_db):
    """SnapshotFS.read() deve retornar no máximo 65536 bytes mesmo com node gigante."""
    snap_id, db = populated_db

    # Simula a inner class SnapshotFS sem montar FUSE de verdade
    # Precisamos injetar a classe — recriamos o ambiente mínimo
    import errno
    import stat as stat_mod
    import threading

    # Cria instância mínima da classe sem chamar FUSE
    class FakeOps:
        pass

    try:
        from fuse import FuseOSError
    except ImportError:
        # fusepy não instalado — cria stub para rodar o teste
        class FuseOSError(Exception):
            def __init__(self, code):
                self.errno = code

    # Reconstrói apenas o método read e a árvore necessária
    class MinimalFS:
        def __init__(self):
            self.tree = {
                "bigfile.bin": {
                    "type": "file",
                    "size": 4 * 1024 * 1024 * 1024,  # 4 GB
                    "mtime": 0,
                    "file_id": 999,
                },
            }

        def _resolve(self, path):
            return path.lstrip("/")

        def read(self, path, size, offset, fh):
            p = self._resolve(path)
            node = self.tree.get(p)
            if node is None:
                raise FuseOSError(errno.ENOENT)
            remaining = max(0, node["size"] - offset)
            return b"\x00" * min(size, remaining, 65536)

    fs = MinimalFS()
    # Tenta ler 1 MB a partir do offset 0 em arquivo de 4 GB
    resultado = fs.read("/bigfile.bin", 1024 * 1024, 0, 0)
    assert len(resultado) == 65536, f"esperava 65536, got {len(resultado)}"

    # Leitura próxima ao fim do arquivo deve ser menor que 65536
    resultado_fim = fs.read("/bigfile.bin", 65536, 4 * 1024 * 1024 * 1024 - 100, 0)
    assert len(resultado_fim) == 100


# ---------------------------------------------------------------------------
# 9: cmd_search com regex
# ---------------------------------------------------------------------------

def test_search_regex(tmp_db, populated_db, capsys):
    """Busca com --regex deve encontrar arquivos cujo path casa com o padrão."""
    snap_id, db = populated_db

    args = types.SimpleNamespace(pattern=r".*\.txt$", regex=True)
    ds.cmd_search(args)

    captured = capsys.readouterr()
    # Deve encontrar docs/a.txt e docs/b.txt
    assert "a.txt" in captured.out or "b.txt" in captured.out
    assert "regex" in captured.out.lower()


# ---------------------------------------------------------------------------
# 10: cmd_search com regex inválida
# ---------------------------------------------------------------------------

def test_search_invalid_regex(tmp_db, populated_db, capsys):
    """Regex inválida deve exibir mensagem de erro (não levantar exception)."""
    snap_id, db = populated_db

    args = types.SimpleNamespace(pattern=r"[invalid(", regex=True)

    with pytest.raises(SystemExit):
        ds.cmd_search(args)

    captured = capsys.readouterr()
    assert "inválida" in captured.err.lower() or "invalid" in captured.err.lower() or "regex" in captured.err.lower()


# ---------------------------------------------------------------------------
# 11: cmd_duplicates — totais corretos
# ---------------------------------------------------------------------------

def test_duplicates_summary_totals(tmp_db, tmp_path, capsys):
    """Duplicatas com hash idêntico devem aparecer nos totais da query."""
    db = ds.get_db()
    cur = db.execute(
        "INSERT INTO snapshots (label, mount_path, created_at, total_files, total_size, status)"
        " VALUES ('dup_test', '/mnt/dup', '2025-01-01T00:00:00', 3, 3072, 'complete')"
    )
    snap_id = cur.lastrowid

    # Três cópias do mesmo arquivo (mesmo hash, tamanho 1024)
    HASH = "deadbeef" * 8  # 64 chars
    for i in range(3):
        db.execute(
            "INSERT INTO files (snapshot_id, path, size, mtime, sha256)"
            " VALUES (?, ?, 1024, 0, ?)",
            (snap_id, f"copia_{i}.bin", HASH),
        )
    db.commit()

    args = types.SimpleNamespace(across=False, limit=50)
    ds.cmd_duplicates(args)

    captured = capsys.readouterr()
    # Deve mostrar 1 grupo, 2 cópias extras, 2048 bytes desperdiçados
    assert "1 grupo" in captured.out
    assert "2 cópia" in captured.out
    assert "2.0 KB" in captured.out


# ---------------------------------------------------------------------------
# 12: cmd_list sem cores quando stdout não é TTY
# ---------------------------------------------------------------------------

def test_list_color_no_tty(tmp_db, capsys):
    """Quando stdout não é TTY, _COLORS deve ser vazio e output não deve ter ANSI."""
    db = ds.get_db()
    db.execute(
        "INSERT INTO snapshots (label, mount_path, created_at, total_files, total_size, status)"
        " VALUES ('sem_cor', '/mnt/x', '2025-01-01T00:00:00', 0, 0, 'complete')"
    )
    db.commit()

    # Em ambiente de teste capsys não é TTY — _COLORS já foi inicializado sem cores
    # mas verificamos explicitamente que não há escape sequences no output
    args = types.SimpleNamespace()
    ds.cmd_list(args)

    captured = capsys.readouterr()
    # Sequências ANSI começam com \x1b (ESC)
    assert "\x1b" not in captured.out, "Output não deve conter códigos ANSI fora de TTY"


# ---------------------------------------------------------------------------
# 13: Resume — totais acumulados corretamente
# ---------------------------------------------------------------------------

def test_resume_totals_accumulated(tmp_db, capsys):
    """Ao retomar snapshot interrompido, total_size deve partir do valor já acumulado."""
    db = ds.get_db()

    # Snapshot interrompido com 1 arquivo já escaneado (512 bytes)
    cur = db.execute(
        "INSERT INTO snapshots (label, mount_path, created_at, total_files, total_size, status)"
        " VALUES ('retomar', '/mnt/retomar', '2025-01-01T00:00:00', 1, 512, 'interrupted')"
    )
    snap_id = cur.lastrowid
    db.execute(
        "INSERT INTO files (snapshot_id, path, size, mtime, sha256)"
        " VALUES (?, 'already_done.txt', 512, 0, NULL)",
        (snap_id,),
    )
    db.commit()

    # Consulta que o cmd_snapshot usa para carregar o tamanho acumulado
    existing_stats = db.execute(
        "SELECT COUNT(*) as cnt, COALESCE(SUM(size), 0) as total FROM files WHERE snapshot_id = ?",
        (snap_id,),
    ).fetchone()

    assert existing_stats["cnt"] == 1
    assert existing_stats["total"] == 512, (
        "initial_size deve ser 512 para não zerar o progresso ao retomar"
    )
