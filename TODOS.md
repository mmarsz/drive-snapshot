# TODOS — drive-snapshot

## Concluídos (v2 — 2026-03-17)

### P0 — Bugs Críticos
- [x] Fix: shutil.move sobrescreve destino silenciosamente → detecção de conflito
- [x] Fix: Validação de path traversal no apply → `_validate_path()` helper

### P1 — Correções Importantes
- [x] Fix: FUSE read() memory bomb → cap 64KB por chamada
- [x] Feature: Scan resumível (Ctrl+C) → SIGINT handler + status column + resume
- [x] Fix: UnicodeDecodeError no scanner → onerror callback + try/except amplo
- [x] Fix: Ordenação de operações no apply → mkdir→move→delete→rmdir via SQL CASE
- [x] Feature: --dry-run no apply

### P2 — Melhorias
- [x] Feature: Logging + audit trail → `_setup_logging()` + `--verbose` flag
- [x] Feature: Duplicates N+1 query fix → batch IN query + defaultdict

### P3 — Delight / Quality of Life
- [x] UX: Rich progress bars (opcional, fallback para \r-based)
- [x] UX: Auto-detect drive label via `lsblk -no LABEL`
- [x] UX: Color-coded 'list' output (verde/amarelo/vermelho por idade)
- [x] UX: Search com regex (`--regex` flag)
- [x] UX: Duplicates summary header (total grupos + cópias extras + espaço desperdiçado)

## Próximos Passos (task_plan.md)

- [ ] Phase 1.2: Incremental snapshots (`update` command)
- [ ] Phase 1.3: Verify / bitrot detection (`verify` command)
- [ ] Phase 1.5: rsync-based apply (`--rsync` flag)
- [ ] Phase 2.1: Smart dedup assistant (`dedup` command)
- [ ] Phase 2.2: Space planner (`plan-move` command)
- [ ] Phase 2.3: Dashboard / status (`status` command)
- [ ] Phase 3.1: Split em Python package
- [ ] Phase 3.3: DB path configurável (XDG + env var)
- [ ] Phase 3.4: Schema migrations system
- [ ] Phase 4.1: Tags e categorias
- [ ] Phase 4.2: rclone integration (Google Drive, S3, etc.)
- [ ] Phase 4.4: Drive replacement workflow (`migrate` command)
