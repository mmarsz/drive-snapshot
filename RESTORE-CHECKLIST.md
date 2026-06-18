# Checklist de formatação & restauração — PC mmarsz

Gerado em 2026-06-18. Backup do `/home` via `tar` no Ventoy (exFAT).

---

## FASE 0 — Antes de formatar (NÃO PULE)

- [ ] **Dedup já rodado** (`dedupe_verify.py` — só remove cópias idênticas confirmadas por `cmp`).
- [ ] **Backup criado**: `tar` de `/home/mmarsz` no Ventoy. Comando:
  ```bash
  tar --exclude=.cache --exclude=.npm --exclude=.cargo --exclude=.rustup \
      --exclude=node_modules --exclude=.venv --exclude=venv \
      --exclude=__pycache__ --exclude='*.pyc' --exclude='.local/share/claude' \
      -cvf /media/mmarsz/Ventoy/home-mmarsz-2026-06-18.tar -C /home mmarsz
  ```
- [ ] **Verificar o tar antes de confiar nele** (lista sem extrair + conta arquivos):
  ```bash
  tar -tvf /media/mmarsz/Ventoy/home-mmarsz-2026-06-18.tar | wc -l
  tar -tvf /media/mmarsz/Ventoy/home-mmarsz-2026-06-18.tar | tail   # vê se terminou íntegro
  ```
- [ ] **Conferir credenciais no tar** (o que NÃO pode faltar):
  ```bash
  tar -tf .../home-mmarsz-2026-06-18.tar | grep -E 'mmarsz/\.ssh/|\.gnupg/|\.git-credentials|\.config/gh/|\.claude' | head
  ```
- [ ] Anotar **lista de pacotes apt** e **snaps** (pra reinstalar igual):
  ```bash
  apt-mark showmanual > /media/mmarsz/Ventoy/apt-manual.txt
  snap list             > /media/mmarsz/Ventoy/snap-list.txt
  pip list --user 2>/dev/null > /media/mmarsz/Ventoy/pip-user.txt
  ```
- [ ] **Tailscale**: anotar nome do nó / chave (infra via Tailscale). Reautenticar depois.
- [ ] Desmontar os drives com segurança antes de desligar:
  ```bash
  sync && udisksctl unmount -b /dev/sda1   # repetir pros outros
  ```

---

## FASE 1 — Instalar o Linux novo

- [ ] Instalar a distro (mesma base Ubuntu 24.04 recomendada — combina com os snaps/pacotes).
- [ ] **Criar o MESMO usuário `mmarsz`** → preserva os paths dos projetos do Claude Code
      (as conversas são indexadas por caminho, ex.: `-home-mmarsz-pessoal-...`).
- [ ] Atualizar: `sudo apt update && sudo apt upgrade -y`

---

## FASE 2 — Restaurar o /home

```bash
# Montar o Ventoy e extrair por cima do home (com o usuário recém-criado)
sudo tar -xvf /media/mmarsz/Ventoy/home-mmarsz-2026-06-18.tar -C /home
sudo chown -R mmarsz:mmarsz /home/mmarsz
```
- [ ] **Permissões SSH** (tar preserva, mas confira):
  ```bash
  chmod 700 ~/.ssh && chmod 600 ~/.ssh/* && chmod 644 ~/.ssh/*.pub 2>/dev/null
  chmod 700 ~/.gnupg
  ```
- [ ] Shell: `chsh -s $(which zsh)` (você usa zsh; `.zshrc` veio no tar).

---

## FASE 3 — Reinstalar apps & toolchains

> Tudo abaixo são **caches/binários** que não foram no backup (regeneráveis).

- [ ] **apt** (a partir do apt-manual.txt): `xargs -a apt-manual.txt sudo apt install -y`
- [ ] **snaps** (a partir do snap-list.txt) — os seus: `ghidra`, `slack`, `thunderbird`,
      `notion-snap-reborn`, `openboardview`:
  ```bash
  sudo snap install ghidra slack thunderbird notion-snap-reborn openboardview
  ```
- [ ] **Rust**: `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
- [ ] **Node/npm**: via apt ou nvm; depois `npm install` nos projetos (node_modules não veio).
- [ ] **Python/uv**: `curl -LsSf https://astral.sh/uv/install.sh | sh`; recriar venvs (`uv sync`/`pip install -r`).
- [ ] **Arduino/PlatformIO**: reabrir os IDEs; eles rebaixam toolchains (`.arduino15`/`.platformio`).
- [ ] **Chrome / VS Code**: instalar; perfis (`~/.config/google-chrome`, `~/.config/Code`) já vieram no tar.
- [ ] **Tailscale**: `curl -fsSL https://tailscale.com/install.sh | sh && sudo tailscale up`

---

## FASE 4 — Docker (trivial — era praticamente vazio)

```bash
sudo apt install -y docker.io          # ou docker-ce
sudo usermod -aG docker mmarsz         # relogar depois
docker pull nginx:alpine               # única imagem que existia
```
- [ ] `docker-compose.yml` dos projetos já vieram no tar do home.

---

## FASE 5 — Claude Code (memória + conversas)

```bash
# Instalar o CLI (reinstala o binário que NÃO foi no backup)
# (use o método oficial atual do Claude Code)
```
Restaurados automaticamente pelo tar do home (não precisa fazer nada além de instalar o CLI):
- `~/.claude/` → conversas (`projects/**/*.jsonl`), memória (`projects/*/memory/`), settings, skills, commands, plugins
- `~/.claude.json` → config + MCP servers
- `~/.claude-mem/` → memória cross-session (SQLite + vetor chroma)
- [ ] Testar: abrir o Claude Code num projeto e ver se o histórico/memória aparecem.
- [ ] Reautenticar MCPs que pedem login interativo (ex.: claude.ai).

---

## FASE 6 — Conferência final

- [ ] `git status` nos repos importantes (ex.: cfo-ai-monorepo) → working tree ok.
- [ ] Chaves SSH funcionam: `ssh -T git@github.com`.
- [ ] (Opcional) Catalogar o `/home` novo e comparar com o snapshot #9 antigo
      pra confirmar que nada essencial sumiu:
  ```bash
  ./drive-snapshot.py snapshot /home/mmarsz --label HOME-novo --exclude .cache \
      --exclude node_modules --exclude .venv --exclude .git
  ./drive-snapshot.py compare 9 <novo_id>
  ```
- [ ] Só então: apagar/reaproveitar o backup tar do Ventoy.
