# TODOS — drive-snapshot

## P0 — Bugs Críticos (Pré-Phase 1)

### Fix: shutil.move sobrescreve destino silenciosamente
- **O que:** Checar se destino existe antes de cada move no apply. Errar com conflito.
- **Por que:** Perda de dados — sobrescreve arquivos sem aviso.
- **Onde:** `drive-snapshot.py:799` — `shutil.move(src, dst)`
- **Fix:** `if os.path.exists(dst): raise ConflictError(...)`
- **Esforço:** S
- **Review:** plan-ceo-review v2 (2026-03-17)

### Fix: Validação de path traversal no apply
- **O que:** Validar que todos os paths de pending_ops resolvem dentro de mount_real.
- **Por que:** `os.path.join(mount_real, dst_path)` ignora mount_real se dst_path é absoluto. Permite escrita em paths arbitrários.
- **Onde:** `drive-snapshot.py:769-797` — loop de apply
- **Fix:** `resolved = os.path.realpath(target); assert resolved.startswith(os.path.realpath(mount_real))`
- **Esforço:** S
- **Review:** plan-ceo-review v2 (2026-03-17)

## P1 — Correções Importantes (Phase 1)

### Fix: FUSE read() memory bomb
- **O que:** `read()` retorna `b"\x00" * size` — para arquivo de 4GB, aloca 4GB na RAM.
- **Por que:** Qualquer programa tentando ler arquivo grande via FUSE mount faz OOM.
- **Onde:** `drive-snapshot.py:551` — `return b"\x00" * min(size, remaining)`
- **Fix:** Cap em `min(size, remaining, 65536)` — retorna no máximo 64KB de zeros por chamada.
- **Esforço:** S
- **Review:** plan-ceo-review v2 (2026-03-17)

### Feature: Scan resumível (Ctrl+C)
- **O que:** Adicionar coluna status em snapshots (scanning/complete/failed). Permitir resume.
- **Por que:** Scan de 10TB interrompido em 70% não deveria recomeçar do zero.
- **Depende de:** Phase 1.2 (incremental snapshots)
- **Esforço:** M
- **Review:** plan-ceo-review v2 (2026-03-17)

### Fix: UnicodeDecodeError no scanner
- **O que:** Tratar encoding errors em nomes de arquivo durante os.walk.
- **Por que:** Drives NTFS/exFAT com nomes non-UTF-8 crasham o scan.
- **Onde:** `drive-snapshot.py:125-129` — os.walk loop
- **Fix:** `onerror` callback no os.walk + try/except com `os.fsencode`
- **Esforço:** S
- **Review:** plan-ceo-review v2 (2026-03-17)

### Fix: Ordenação de operações no apply
- **O que:** Ordenar pending_ops por tipo (mkdir → move → delete → rmdir) em vez de created_at.
- **Por que:** Criar pasta e mover arquivo pra dentro dela falha se move executa antes de mkdir.
- **Onde:** `drive-snapshot.py:753-754` — ORDER BY created_at
- **Fix:** Sort por tipo + dependências de path
- **Esforço:** M
- **Review:** plan-ceo-review v2 (2026-03-17)

### Feature: --dry-run no apply
- **O que:** Flag para mostrar o que apply faria sem executar.
- **Por que:** Usuário precisa de confiança antes de operações destrutivas no drive real.
- **Esforço:** S
- **Review:** plan-ceo-review v2 (2026-03-17)

## P2 — Melhorias (Phase 2-3)

### Feature: Logging + audit trail
- **O que:** Usar módulo logging do Python, log em arquivo, especialmente para apply.
- **Por que:** Se arquivos somem após apply, precisa rastrear o que aconteceu.
- **Esforço:** M
- **Review:** plan-ceo-review v2 (2026-03-17)

### Feature: Duplicates N+1 query fix
- **O que:** Usar JOIN em vez de query-por-grupo no cmd_duplicates.
- **Onde:** `drive-snapshot.py:325-331` — query dentro de loop
- **Esforço:** S
- **Review:** plan-ceo-review v2 (2026-03-17)

## P3 — Delight / Quality of Life

### UX: Rich progress bars
- **O que:** Trocar progresso \r-based por `rich.progress` com barra bonita, ETA, throughput.
- **Esforço:** S (20 min)
- **Review:** plan-ceo-review v2 (2026-03-17)

### UX: Auto-detect drive label
- **O que:** Detectar label do drive via `lsblk -o LABEL` ao invés de exigir `--label`.
- **Esforço:** S (15 min)
- **Review:** plan-ceo-review v2 (2026-03-17)

### UX: Color-coded 'list' output
- **O que:** Snapshots antigos (>30d) em amarelo, muito antigos (>90d) em vermelho, recentes em verde.
- **Esforço:** S (15 min)
- **Review:** plan-ceo-review v2 (2026-03-17)

### UX: Search com regex
- **O que:** Suporte a regex no search (`search ".*\.raw$"`).
- **Esforço:** S (20 min)
- **Review:** plan-ceo-review v2 (2026-03-17)

### UX: Duplicates summary header
- **O que:** Header com total de espaço desperdiçado e contagem de grupos/cópias.
- **Esforço:** S (10 min)
- **Review:** plan-ceo-review v2 (2026-03-17)
