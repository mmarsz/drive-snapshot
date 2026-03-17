# drive-snapshot

Catalog files across multiple HDDs/SSDs that can't all be connected at the same time. Take snapshots, find duplicates by SHA256 hash, compare drives, and organize files visually through your file manager — even with drives disconnected.

Handles NTFS, exFAT, ext4 and other filesystems gracefully — permission errors, encoding issues, and I/O errors are logged and skipped, never crash the scan.

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

# Optional: pretty progress bars
pip install rich
```

Requires Python 3.8+ and `libfuse` (pre-installed on most Linux distros).

## Usage

### Take a snapshot

```bash
# Scan a mounted drive (hashes all files with SHA256)
# Label is auto-detected from filesystem if not provided
./drive-snapshot.py snapshot /mnt/hd-photos

# Explicit label
./drive-snapshot.py snapshot /mnt/hd-photos --label "HD-Photos"

# Quick scan without hashing (faster, but no duplicate detection)
./drive-snapshot.py snapshot /mnt/hd-photos --no-hash
```

**Resumable scans:** If you interrupt a scan with Ctrl+C, progress is saved. Run the same `snapshot` command again to resume from where you left off.

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

Output is color-coded by age: green (< 7 days), yellow (30-90 days), red (> 90 days).

### Search files

```bash
# Simple text search (LIKE)
./drive-snapshot.py search "vacation-2024"

# Regex search
./drive-snapshot.py search ".*\.raw$" --regex
./drive-snapshot.py search "^DCIM/" --regex

# List files from a specific snapshot
./drive-snapshot.py files 1 --sort size --limit 20
```

### Find duplicates

```bash
# All duplicates across all snapshots
./drive-snapshot.py duplicates

# Only duplicates that exist on different drives
./drive-snapshot.py duplicates --across
```
```
Duplicatas em todos os snapshots: 1247 grupos, 3891 cópias extras (42.3 GB desperdiçado)

  Hash: a1b2c3d4e5f6...  Tamanho: 15.2 MB  Cópias: 3 (em 2 snapshot(s))
    [HD-Photos#1] photos/vacation/IMG_1234.jpg
    [HD-Photos#1] backup/old/IMG_1234.jpg
    [SSD-Backup#2] photos/IMG_1234.jpg
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

# Preview changes without executing (dry run)
./drive-snapshot.py apply 1 /mnt/hd-photos --dry-run

# Reconnect the drive, then apply changes to the real filesystem
./drive-snapshot.py apply 1 /mnt/hd-photos
```

The `apply` command:
- Shows a preview and asks for confirmation before touching any real files
- Executes operations in safe order: mkdir → move → delete → rmdir
- Detects destination conflicts (won't overwrite existing files)
- Validates all paths to prevent traversal outside the mount point
- Supports `--dry-run` to preview without executing

### Export

```bash
./drive-snapshot.py export 1 --format csv
./drive-snapshot.py export 1 --format json
```

### Verbose logging

```bash
# Any command with --verbose shows detailed logs
./drive-snapshot.py snapshot /mnt/hd-photos --verbose

# Logs are always written to ~/.local/share/drive-snapshot/drive-snapshot.log
# Apply operations are logged as an audit trail
```

## Data storage

Everything is stored in `snapshots.db` (SQLite with WAL mode) in the same directory as the script. The database is portable — you can copy it to another machine.

## Safety features

- **Path traversal protection** — pending operations are validated to stay within the mount point
- **Conflict detection** — apply won't overwrite existing files (skips and reports)
- **Dry run mode** — preview all changes before executing
- **Resumable scans** — Ctrl+C saves progress, next run resumes
- **Operation ordering** — mkdir before move, delete before rmdir
- **Cross-filesystem resilience** — handles NTFS/exFAT encoding errors, permission issues, and I/O errors gracefully
- **Audit logging** — all apply operations logged to file

## Optional dependencies

| Package | Feature | Install |
|---------|---------|---------|
| `fusepy` | Virtual FUSE mount | `pip install fusepy` |
| `rich` | Pretty progress bars | `pip install rich` |

All other features work with just Python stdlib.

## Limitations

- **File content is not stored** — only metadata (path, size, modification time, SHA256). The virtual mount shows the directory structure but files contain placeholder data.
- **Hashing large drives takes time** — use `--no-hash` for a quick catalog, or let it run. Progress is shown with ETA.
- **FUSE mount requires `fusepy`** — all other features work with just Python stdlib.

## Roadmap

See [task_plan.md](task_plan.md) for the full v2 roadmap including:
- Incremental snapshots (only re-hash changed files)
- Verify / bitrot detection
- rsync-based apply
- Smart dedup assistant
- Space planner
- rclone integration (Google Drive, Dropbox, S3, and 40+ more)
- Tags and categories
- Drive migration workflow

## License

MIT
