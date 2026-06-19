# Referência de restauração — inventário do PC (2026-06-19)

Lista de tudo instalado, pra você **decidir o que restaurar vs reinstalar**.
As listas brutas estão no backup em `/mnt/backup-home/system-extras/`.

Legenda: ✅ já no backup (restaura) · 🔁 reinstalar do zero · ⚠️ atenção

---

## Resumo rápido — o que está no backup
- ✅ **`/home/mmarsz` inteiro** (198 GB) — dados, dotfiles, projetos, `.ssh`, `.gnupg`, Chrome (perfil+senhas+keyring), Claude (`.claude`/`.claude.json`/`.claude-mem`), Flatpak app data (`~/.var/app`), nvm, systemd user units.
- ✅ **`/etc`** (cópia em `system-extras/etc/`) — configs de sistema p/ referência.
- ✅ **`/usr/local`** (cópia em `system-extras/local/`) — inclui seus scripts `larc-vpn*`, `ollama`, `arduino-cli`, `palera1n`, `piper`, `ueberzug`.
- ✅ **Manifestos** de todos os pacotes/apps (`system-extras/*.txt`).

## ⚠️ O que NÃO está no backup (decisão sua)
- ⚠️ **`/opt/Xilinx` (21 GB)** — NÃO coube (backup só tinha 11 GB livres). Vivado/Vitis é gigante e chato de reinstalar. **Se precisa, copie pra OUTRO drive antes de formatar.**
- 🔁 Resto do `/opt` (~3 GB, reinstalável): bytedance 1.7G, calibre, ti 431M, google, arduino, cutter, flexbv, piper, openbao, smfp-common, containerd.
- 🔁 **Imagens Docker** — só `nginx:alpine` (62 MB), 0 containers, 0 volumes → `docker pull nginx:alpine`.
- 🔁 Pacotes apt/snap/flatpak (listas abaixo) — reinstalar.

---

## Docker
**Trivial.** Imagens ficam em `/var/lib/docker` (fora do /home, não vêm no backup), mas só há:
```
nginx:alpine  (62 MB)   |   containers: 0   |   volumes: 0
```
Restaurar: `sudo apt install docker.io && docker pull nginx:alpine`. Seus `docker-compose.yml` estão no /home (no backup).

## APT — 261 pacotes manuais
Lista: `system-extras/apt-manual.txt`. Reinstalar em massa:
```bash
xargs -a apt-manual.txt sudo apt install -y
```

## Snaps (20) — 🔁 reinstalar
Seus (não-base): **ghidra, slack, thunderbird, notion-snap-reborn, openboardview**.
```bash
sudo snap install ghidra slack thunderbird notion-snap-reborn openboardview
```

## Flatpaks (32) — 🔁 reinstalar do zero (sua preferência)
App data já está no backup (`~/.var/app`), mas se reinstalar limpo:
**Discord, Spotify, Stremio, Cura (ultimaker), Obsidian, ImHex, Ghidra, KiCad, Censor, NetworkDisplays.**
```bash
flatpak install flathub com.discordapp.Discord com.spotify.Client com.stremio.Stremio \
  com.ultimaker.cura md.obsidian.Obsidian net.werwolv.ImHex org.kicad.KiCad
```
⚠️ **Obsidian**: os vaults/notas estão no /home (no backup) — só reinstalar o app e reabrir o vault.

## /opt — apps grandes (NÃO no backup)
| App | Tamanho | Ação |
|---|---|---|
| **Xilinx** (Vivado/Vitis) | **21 GB** | ⚠️ copiar p/ outro drive OU reinstalar (chato) |
| bytedance | 1.7 GB | 🔁 reinstalar |
| calibre | 626 MB | 🔁 reinstalar |
| ti (Code Composer?) | 431 MB | 🔁 reinstalar |
| google, arduino, cutter, flexbv, piper, openbao | <500 MB cada | 🔁 reinstalar |

## /usr/local/bin — ✅ copiado (system-extras/local/bin/)
Scripts custom **`larc-vpn` / `larc-vpn-up/down/status`** (seu VPN!), `ollama`, `arduino-cli`, `arduino-ide`, `cutter`, `palera1n`, `piper`, `ueberzug`.
→ Restaurar: copiar de volta pra `/usr/local/bin/` (os scripts) ou reinstalar os binários grandes.

## npm global (Node v20.20.0 via nvm) — 🔁
`@bitwarden/cli`, `@google/gemini-cli`, `@google/generative-ai`, `@openai/codex`, `claudekit`, `firecrawl-mcp`, `corepack`.
```bash
npm i -g @bitwarden/cli @google/gemini-cli @openai/codex claudekit firecrawl-mcp
```

## pipx — 🔁
gnome-extensions-cli, pip-audit, pyserial, syncedlyrics, uefi-firmware, vulture.

## cargo — 🔁
claude-tmux, ncspot, ssd-flash-id.

## VS Code — 27 extensões (`system-extras/vscode-extensions.txt`) — 🔁
Embedded/MCU pesado (cortex-debug, platformio, pico, cpptools), python/jupyter, claude-code, copilot, roo-cline.
```bash
xargs -L1 code --install-extension < vscode-extensions.txt
```

## Credenciais / chaves — ✅ tudo no backup (/home)
`.ssh` (chaves+config+known_hosts), `.gnupg`, `gh`, `.git-credentials`, Chrome keyring (`~/.local/share/keyrings`), nvm.
⚠️ As senhas do Chrome decifram com o keyring **+ mesma senha de login** (ou use Bitwarden CLI / Chrome Sync).

## Rede / Tailscale
- Wi-Fi: **0 conexões salvas** no NetworkManager (você usa cabo) — nada a restaurar.
- `/etc/hosts` tem `auth.contaxis.app` via Tailscale (no backup `system-extras/etc/`).
- **Tailscale**: reautenticar na máquina nova (`sudo tailscale up`). Estado antigo em `/var/lib/tailscale` (não crítico).
