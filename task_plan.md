# drive-snapshot v2 — Plano de Evolução

## Visão

**Snapshot anything, organize offline, apply anywhere.**

Catalogar qualquer fonte de dados — drives locais (NTFS, exFAT, ext4) e remotes via rclone (Google Drive, Dropbox, OneDrive, S3, SFTP, e 40+ outros). Organizar arquivos offline no file manager e aplicar mudanças quando reconectar — de forma resiliente, sem crashar em permissões, I/O errors, ou filesystems exóticos.

## Estado Atual (v1)

- Script único (`drive-snapshot.py`, 929 linhas)
- 11 comandos CLI: snapshot, list, files, search, duplicates, compare, export, mount, pending, apply, delete
- SQLite com WAL (3 tabelas: snapshots, files, pending_ops)
- FUSE mount com rename/move/delete gravando pending_ops
- Hash SHA256 em chunks de 1MB
- Sem testes, sem packaging, sem configuração de DB

## Fase 1 — Resiliência e Fundamentos

### 1.1 Cross-Filesystem Resilience
**Problema:** Scan de NTFS/exFAT/ext4 crasha em permissões, caracteres especiais, symlinks quebrados, I/O errors.

**Implementação:**
- Wrap todo `os.stat()` / `os.walk()` / `open()` com error handling granular
- Categorizar erros: `PermissionError`, `OSError(errno.EIO)`, `UnicodeDecodeError`, `FileNotFoundError` (race condition)
- Gravar arquivos inacessíveis em tabela `scan_errors(snapshot_id, path, error_type, error_msg)`
- Flag `--force` para ignorar todos os erros e continuar
- Relatório no final: "X arquivos escaneados, Y inacessíveis (Z permissão, W I/O error)"
- Tratar nomes de arquivo com encoding estranho (latin-1, shift-jis) via `os.fsencode`/`os.fsdecode`
- Seguir symlinks: `--follow-symlinks` (off por padrão, reportar symlinks encontrados)
- Detectar e pular filesystems especiais montados dentro do scan (procfs, sysfs, devfs)

### 1.2 Incremental Snapshots
**Problema:** Re-escanear 10TB demora horas. Precisa de update incremental.

**Implementação:**
- Novo comando: `update <snapshot_id> <caminho>`
- Comparar mtime+size dos arquivos existentes no banco vs filesystem
- Só re-hashear arquivos novos ou modificados (mtime/size mudou)
- Marcar arquivos removidos do filesystem como `deleted_at` (soft delete)
- Relatório: "X novos, Y modificados, Z removidos, W inalterados (pulados)"
- Adicionar coluna `files.updated_at` para tracking

### 1.3 Verify / Bitrot Detection
**Problema:** Drives degradam silenciosamente. Precisa verificar integridade.

**Implementação:**
- Novo comando: `verify <snapshot_id> <caminho>`
- Re-hashear arquivos do drive conectado e comparar com snapshot
- Categorias: `OK`, `MODIFIED` (hash diferente), `MISSING`, `NEW` (não estava no snapshot), `ERROR` (não conseguiu ler)
- Output: relatório com contagem + lista de arquivos por categoria
- Flag `--fix`: atualiza o snapshot com os hashes novos após verificação
- Flag `--only-errors`: mostra apenas problemas

### 1.4 Fix Apply Safety
**Problema:** `apply` deleta TODAS as pending_ops mesmo em falha parcial. Perde o registro dos erros.

**Implementação:**
- Marcar cada op como `applied_at` ou `error_msg` em vez de deletar em batch
- Flag `--dry-run`: mostra o que faria sem executar
- Ordenar operações: mkdir primeiro, depois move, depois delete, depois rmdir
- Rollback log: gravar o que foi feito para desfazer se necessário
- Só limpar ops com `applied_at` preenchido
- Manter ops com erro para retry manual

### 1.5 rsync-based Apply
**Problema:** `shutil.move` é frágil para volumes grandes e cross-filesystem.

**Implementação:**
- Flag `--rsync` no apply: gera script rsync em vez de executar diretamente
- Para moves: `rsync -avP --remove-source-files src dst`
- Para deletes: arquivo de exclusão para `rsync --delete`
- Output: gerar script `.sh` que o usuário pode revisar e executar
- Alternativa: executar rsync diretamente com `--dry-run` primeiro
- Manter fallback com shutil para operações simples

## Fase 2 — Features de Uso Diário

### 2.1 Smart Dedup Assistant
**Problema:** `duplicates` encontra duplicatas mas não ajuda a decidir o que manter.

**Implementação:**
- Novo comando: `dedup [--across] [--auto-keep STRATEGY]`
- Estratégias de keep: `newest` (mtime mais recente), `largest-drive` (manter no drive com mais espaço), `specific-drive <id>` (manter no drive X)
- Output interativo: mostra grupo por grupo, pede confirmação
- Gerar pending_ops de delete para as cópias descartadas
- Preview do espaço recuperável por estratégia
- Flag `--generate-script`: gera script de deleção em vez de pending_ops

### 2.2 Space Planner
**Problema:** "Quero consolidar 3 drives em 2. Como?"

**Implementação:**
- Novo comando: `plan-move --from <ids> --to <id> [--max-usage 90%]`
- Calcula: espaço total necessário, espaço disponível no destino, conflitos de nome
- Mostra: o que vai pra onde, o que já existe (skip), o que é duplicata
- Gera: pending_ops ou script rsync para executar
- Detecta: "Drive #2 não tem espaço suficiente — faltam X GB"

### 2.3 Dashboard / Summary
**Problema:** Sem visão geral de todos os drives.

**Implementação:**
- Novo comando: `status` (ou `dashboard`)
- Mostra: tabela com todos snapshots, espaço total, overlap entre drives, último scan, idade do snapshot
- Métricas: total de arquivos únicos vs duplicatas, espaço desperdiçado em duplicatas
- Alerta: snapshots antigos (>30 dias), drives com muito overlap

## Fase 3 — Arquitetura e Qualidade

### 3.1 Split em Package Python
**Problema:** 929 linhas em 1 arquivo vai virar 2000+ com as features novas.

**Estrutura:**
```
drive-snapshot/
├── pyproject.toml
├── src/
│   └── drive_snapshot/
│       ├── __init__.py
│       ├── __main__.py       # entry point (python -m drive_snapshot)
│       ├── cli.py            # argparse + dispatch
│       ├── db.py             # get_db, migrations, schema
│       ├── scanner.py        # snapshot, update, verify
│       ├── queries.py        # search, duplicates, compare, status
│       ├── fuse_mount.py     # SnapshotFS + cmd_mount
│       ├── apply.py          # apply, pending, rsync integration
│       ├── export.py         # csv, json export
│       ├── planner.py        # dedup assistant, space planner
│       ├── cloud/
│       │   ├── __init__.py
│       │   └── gdrive.py     # Google Drive snapshot
│       └── utils.py          # fmt_size, fmt_time, hash_file
├── tests/
│   ├── test_scanner.py
│   ├── test_queries.py
│   ├── test_fuse.py
│   ├── test_apply.py
│   └── test_planner.py
└── README.md
```

### 3.2 Testes
**Cobertura mínima:**
- Scanner: scan com erros de permissão, incremental update, arquivos com nomes unicode
- Queries: duplicatas, compare com snapshots vazios, search case-insensitive
- Apply: dry-run, falha parcial mantém ops com erro, ordenação de operações
- FUSE: rename de diretório move filhos, delete atualiza parent
- Planner: consolidação com espaço insuficiente, dedup com estratégias

### 3.3 DB Path Configurável
- Flag global `--db /path/to/snapshots.db`
- Env var `DRIVE_SNAPSHOT_DB`
- Default: `~/.local/share/drive-snapshot/snapshots.db` (XDG compliant)
- Migração automática do DB antigo (no diretório do script) para novo local

### 3.4 Schema Migrations
- Versionamento do schema com tabela `schema_version`
- Migrations incrementais (1→2, 2→3, etc.)
- Auto-migrate no startup
- Novas colunas: `files.updated_at`, `files.deleted_at`, `scan_errors`, `schema_version`

## Fase 4 — Expansão

### 4.1 Tags e Categorias
**Implementação:**
- Nova tabela: `tags(id, name)` + `file_tags(file_id, tag_id)`
- Comandos: `tag <snapshot_id> <pattern> --tag photos`, `search --tag photos`
- Auto-tag por extensão: `.jpg/.png` → `#images`, `.mp4/.mkv` → `#video`
- Tags persistem entre updates de snapshot

### 4.2 rclone Integration (Google Drive, Dropbox, S3, e 40+ outros)
**Conceito:** Usar rclone como backend universal para qualquer storage remoto. Em vez de implementar OAuth2, API clients, etc. para cada serviço, delegamos tudo ao rclone — que o usuário já pode ter configurado.

**Arquitetura — Source Abstraction:**
```python
class Source(Protocol):
    def scan(self, path: str) -> Iterator[FileEntry]: ...
    def verify(self, path: str, expected_hash: str) -> VerifyResult: ...
    def apply_ops(self, ops: list[PendingOp]) -> ApplyResult: ...

class LocalSource(Source):
    """Acesso direto ao filesystem. Código atual."""

class RcloneSource(Source):
    """Qualquer remote que rclone suporta."""
```

**Implementação RcloneSource:**
- Scan: `rclone lsjson --recursive --hash remote:path` → JSON com nome, tamanho, modTime, hashes
- Verify: `rclone check remote:path --one-way` ou re-scan + compare hashes
- Apply:
  - Move → `rclone move remote:src remote:dst`
  - Delete → `rclone delete remote:path` (ou `rclone deletefile`)
  - Copy/download → `rclone copy remote:path /local/path`
  - Upload → `rclone copy /local/path remote:path`
- Requer: rclone instalado e configurado (`rclone config` já feito pelo usuário)
- Snapshot command: `snapshot --rclone gdrive:Photos --label "Google Photos"`
- Detecta remotes disponíveis: `rclone listremotes`
- Tabela: `snapshots.source_type` (local, rclone) + `snapshots.source_config` (JSON: remote name, path, rclone flags)

### 4.3 Web UI (Futuro)
- Lightweight local web server (Flask/FastAPI)
- Visualização: mapa de drives, espaço por drive, duplicatas com visual diff
- Drag-and-drop para planejar moves entre drives
- Timeline de snapshots
- **Escopo para depois** — não incluir na v2 inicial

### 4.4 Drive Replacement Workflow
- Novo comando: `migrate --from <id_dying> --to <id_new>`
- Identifica arquivos únicos no drive morrendo (não existem em nenhum outro snapshot)
- Gera plano de cópia com rsync
- Verifica integridade pós-cópia
- Marca drive antigo como "retired"

## Ordem de Implementação

```
Fase 1 (fundamentos):
  1.1 Cross-FS resilience   ←  primeiro, afeta tudo
  1.2 Incremental snapshots
  1.4 Fix apply safety
  1.5 rsync-based apply
  1.3 Verify / bitrot

Fase 2 (uso diário):
  2.3 Dashboard / status
  2.1 Smart dedup
  2.2 Space planner

Fase 3 (arquitetura):
  3.1 Split em package      ←  fazer ANTES da fase 2 se possível
  3.4 Schema migrations
  3.3 DB path configurável
  3.2 Testes

Fase 4 (expansão):
  4.1 Tags
  4.2 Google Drive snapshot
  4.4 Drive migration
  4.3 Web UI (futuro)
```

**Nota:** Fase 3.1 (split em package) idealmente deveria vir ANTES das features novas para não ter que refatorar 2000 linhas depois. Mas pode ser feito em paralelo se o split for cirúrgico.

## Decisões em Aberto

1. **DB location default:** `~/.local/share/drive-snapshot/` (XDG) ou manter no diretório do script?
2. **rclone dependency:** Obrigatório (hard dependency) ou opcional (só para remotes)?
3. **rsync apply:** Gerar script ou executar diretamente?
4. **Web UI:** Flask ou FastAPI? Incluir na v2 ou postergar?
5. **Split timing:** Antes das features (mais trabalho agora, menos refactor depois) ou depois?
6. **Source abstraction timing:** Introduzir na Fase 3 (com split) ou na Fase 4 (com rclone)?
