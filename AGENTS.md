# AGENTS.md — drive-snapshot

## hcom (multi-agent)

Os agentes deste projeto rodam num **workspace hcom isolado** em `~/.hcom-projects/drive-snapshot/`, não no DB global (`~/.hcom/`).

**Toda intereração hcom precisa prefixar `HCOM_DIR`:**

```bash
HCOM_DIR=~/.hcom-projects/drive-snapshot hcom list -v
HCOM_DIR=~/.hcom-projects/drive-snapshot hcom send @bone @nave --intent request --name <seu-nome> -- "texto"
HCOM_DIR=~/.hcom-projects/drive-snapshot hcom events --last 20
HCOM_DIR=~/.hcom-projects/drive-snapshot hcom term <nome>
```

Sem `HCOM_DIR`, o `hcom list` mostra "No active agents" — os agentes do projeto são invisíveis ao DB global.

### Aliases sugeridos

```bash
# adicionar ao ~/.zshrc
alias dshcom='HCOM_DIR=~/.hcom-projects/drive-snapshot hcom'
# uso: dshcom list -v ; dshcom send @bone -- "..." ; etc.
```

### Agentes ativos (2026-07-14)

- **bone** — devin-cli, listening, dir `~/drive-snapshot`
- **nave** — devin-cli, listening, dir `~/drive-snapshot`
