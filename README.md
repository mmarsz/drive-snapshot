# drive-snapshot

Catalog files across multiple HDDs/SSDs that can't all be connected at the same time. Take snapshots, find duplicates by SHA256 hash, compare drives, and organize files visually through your file manager — even with drives disconnected.

## The problem

You have several drives but limited ports/bays. You need to:
- Know what's on each drive without having them all plugged in
- Find duplicate files across drives
- Reorganize files using your file manager, then apply changes later when the drive is reconnected

## How it works

```
1. Connect a drive → take a snapshot (catalogs all files + SHA256 hashes)
2. Disconnect drive → snapshots persist in a local SQLite database
3. Repeat for other drives
4. Query, compare, find duplicates — all offline
5. Mount a snapshot as a virtual folder (FUSE) → browse in Nautilus/Dolphin/Thunar
6. Rename, move, delete files in the virtual mount
7. Reconnect the drive → apply all pending changes to the real filesystem
```

## Install

```bash
git clone https://github.com/mmarsz/drive-snapshot.git
cd drive-snapshot

# Only dependency (for virtual mount feature)
pip install fusepy
```

Requires Python 3.8+ and `libfuse` (pre-installed on most Linux distros).

## Usage

### Take a snapshot

```bash
# Scan a mounted drive (hashes all files with SHA256)
./drive-snapshot.py snapshot /mnt/hd-photos --label "HD-Photos"

# Quick scan without hashing (faster, but no duplicate detection)
./drive-snapshot.py snapshot /mnt/hd-photos --label "HD-Photos" --no-hash
```

### List snapshots

```bash
./drive-snapshot.py list
```
```
  ID  Label                  Arquivos     Tamanho                  Data  Mount
-----------------------------------------------------------------------------------------------
   1  HD-Photos                 12430     85.2 GB   2026-03-13 14:30:00  /mnt/hd-photos
   2  SSD-Backup                 8291     42.1 GB   2026-03-13 15:00:00  /mnt/ssd-backup
```

### Search files

```bash
./drive-snapshot.py search "vacation-2024"
./drive-snapshot.py files 1 --sort size --limit 20
```

### Find duplicates

```bash
# All duplicates across all snapshots
./drive-snapshot.py duplicates

# Only duplicates that exist on different drives
./drive-snapshot.py duplicates --across
```

### Compare two drives

```bash
./drive-snapshot.py compare 1 2
```
```
  In common (same content):    5420 files  (32.1 GB)
  Only in #1 (HD-Photos):      7010 files  (53.1 GB)
  Only in #2 (SSD-Backup):     2871 files  (10.0 GB)

  Same path, different content: 43
```

### Virtual mount (FUSE)

Mount a snapshot as a browsable folder — works even with the drive disconnected:

```bash
./drive-snapshot.py mount 1 /tmp/hd-photos

# Open in your file manager
nautilus /tmp/hd-photos     # GNOME
dolphin /tmp/hd-photos      # KDE
thunar /tmp/hd-photos       # XFCE
```

Inside the file manager you can:
- **Browse** the full directory tree
- **Rename** files and folders
- **Move** files between folders (drag & drop)
- **Delete** files

All changes are recorded as **pending operations**. File contents are not available (the drive is disconnected), but the full structure is navigable.

```bash
# Unmount when done
fusermount -u /tmp/hd-photos
```

### Review and apply changes

```bash
# See what you changed
./drive-snapshot.py pending 1

# Reconnect the drive, then apply changes to the real filesystem
./drive-snapshot.py apply 1 /mnt/hd-photos
```

The `apply` command shows a preview and asks for confirmation before touching any real files.

### Export

```bash
./drive-snapshot.py export 1 --format csv
./drive-snapshot.py export 1 --format json
```

## Data storage

Everything is stored in `snapshots.db` (SQLite) in the same directory as the script. The database is portable — you can copy it to another machine.

## Limitations

- **File content is not stored** — only metadata (path, size, modification time, SHA256). The virtual mount shows the directory structure but files contain placeholder data.
- **Hashing large drives takes time** — use `--no-hash` for a quick catalog, or let it run. Progress is shown with ETA.
- **FUSE mount requires `fusepy`** — all other features work with just Python stdlib.

## License

MIT
