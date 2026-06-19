# Backup final antes de formatar — passo a passo

## 0. Fechar o que escreve no /home (manual)
- Feche **todas as sessões do Claude Code**, **Chrome**, **VS Code**.

## 1. Parar o worker do claude-mem (backup consistente do .db)
```bash
npx claude-mem stop
```

## 2. Conectar o Kingston (CABO NOVO!) e descobrir a letra
```bash
lsblk -o NAME,SIZE,MODEL,LABEL,MOUNTPOINT | grep -iE 'kingston|backup-home'
# anote se ficou sda1 / sdb1 (label = backup-home)
```

## 3. Montar (troque sdX pela letra do passo 2)
```bash
sudo umount /media/mmarsz/backup-home 2>/dev/null   # se auto-montou
sudo fsck.ext4 -f -y /dev/sdX1                       # rápido se desmontou limpo
sudo mkdir -p /mnt/backup-home
sudo mount -o rw,noatime,nobarrier,commit=60 /dev/sdX1 /mnt/backup-home
```

## 4. Rsync final do /home (copia só o que mudou)
```bash
sudo rsync -aAXH --info=progress2 \
  --exclude=.cache --exclude=.npm --exclude=.cargo --exclude=.rustup \
  --exclude=node_modules --exclude=.venv --exclude=venv --exclude=__pycache__ \
  --exclude='*.pyc' --exclude='.local/share/claude' \
  /home/mmarsz /mnt/backup-home/
```

## 5. Verificar (deve listar ~nada)
```bash
sudo rsync -aAXHn -i \
  --exclude=.cache --exclude=.npm --exclude=.cargo --exclude=.rustup \
  --exclude=node_modules --exclude=.venv --exclude=venv --exclude=__pycache__ \
  --exclude='*.pyc' --exclude='.local/share/claude' \
  /home/mmarsz /mnt/backup-home/ | grep -vE '^\.|/$' | head
# se vier vazio (ou só logs/.db), backup 100% em dia
```

## 6. Desmontar limpo os dois drives
```bash
sync
sudo umount /mnt/backup-home && sudo udisksctl power-off -b /dev/sdX   # Kingston (/home)
# Patriot (Xilinx) — feche o Nautilus antes:
sudo umount /media/mmarsz/Ventoy && sudo udisksctl power-off -b /dev/sdY
```

## 7. Conferência antes de formatar
- [ ] Kingston tem `/home` + `system-extras/` (RESTORE-REFERENCE.md, etc, usr/local, manifestos)
- [ ] Patriot tem `xilinx-opt-2026-06-19.tar`
- [ ] Repo `drive-snapshot` no GitHub (RESTORE-CHECKLIST.md + RESTORE-REFERENCE.md)
- ✅ Pode formatar. Restauração: siga `RESTORE-CHECKLIST.md` e `RESTORE-REFERENCE.md`.

> Restaurar /home (na máquina nova, usuário mmarsz): `sudo rsync -aAXH /mnt/backup-home/mmarsz/ /home/mmarsz/`
> Restaurar Xilinx: `sudo tar -xf xilinx-opt-2026-06-19.tar -C /opt`
