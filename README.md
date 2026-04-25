# 📸 MediaSorter

**Professional media organizer — permanent metadata timestamps, parallel processing, real‑time progress.**

![Python](https://img.shields.io/badge/python-3.8+-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)
![Platform](https://img.shields.io/badge/platform-Linux%20%7C%20macOS%20%7C%20Windows-lightgrey)

---

## 🌟 Why MediaSorter?

Most tools only rename files. **MediaSorter** permanently writes the creation date into the file’s metadata (EXIF for images, QuickTime for videos) and then renames to a clean, sortable pattern. Fast, duplicate‑safe, and future‑proof.

---

## 🚀 Features

- ⏱️ **Permanent timestamp** – Embedded in metadata, never lost.
- 🏷️ **Smart rename** – `YYYYMMDD_HHMMSS__LABEL__UID.ext`
- 🧬 **Duplicate detection** – Content‑based hash; duplicates get `__DUPn` suffix.
- 🔁 **Parallel processing** – Up to 8× faster with thread workers.
- 📊 **Real‑time progress** – Elapsed, ETA, percentage.
- 🛡️ **Timestamp validation** – Rejects invalid dates, auto‑fallback to `mtime`.
- 📝 **Reports** – Console summary + optional JSON.
- 📁 **Date folders** – Optional `YYYY/MM` structure.
- 🚫 **Skip processed** – Marker comment avoids repeated work.
- 🎯 **Dry‑run** – Preview changes safely.
- 🔧 **20+ formats** – `.jpg`, `.png`, `.heic`, `.mp4`, `.mov`, `.mkv`, and more.

---

## 📦 Requirements

- **Python 3.8+**
- **[ExifTool](https://exiftool.org)** (required)
- Optional: `tqdm`, `xxhash` (smoother progress & faster hashing)

---

## 🔧 Installation

```bash
git clone https://github.com/yourusername/MediaSorter.git
cd MediaSorter
pip install tqdm xxhash   # optional
```

---

## 🏃 Quick Start

```bash
# Dry‑run (safe preview)
python MediaSorter.py --target /path/to/media --label Phone --verbose

# Apply changes permanently
python MediaSorter.py --target /path/to/media --label Phone --apply

# Full power
python MediaSorter.py --target /archive --label Camera --apply --workers 8 --date-folder --skip-processed --json-report report.json
```

---

## ⚙️ Options

| Flag | Description |
|------|-------------|
| `--target` | **(Required)** Directory containing media files |
| `--label` | Source label for filenames (default: `Media`) |
| `--apply` | Actually rename & write metadata (otherwise dry‑run) |
| `--verbose` | Detailed per‑file output |
| `--skip-processed` | Skip files already processed by this script |
| `--force-metadata` | Rewrite metadata even if already present |
| `--date-folder` | Sort into `YYYY/MM` subfolders |
| `--require-metadata` | Abort rename if metadata write fails |
| `--workers` | Number of parallel threads (default: `4`) |
| `--json-report` | Save detailed report to a JSON file |

---

## ⚙️ How It Works

1. **Extract timestamp** – Reads metadata in priority order; falls back to `mtime` if invalid.
2. **Write timestamp** – Embeds timestamp and a marker comment into the file using `exiftool`.
3. **Compute content UID** – Fast hash from first/middle/last chunks + file size.
4. **Rename** – Creates new name with timestamp, label, UID; duplicates receive a `__DUPn` suffix.

---

## 📊 Benchmarks (Approximate)

*Mixed 1000 files on SSD*

| Workers | Time | Speedup |
|---------|------|---------|
| 1 | ~90 s | 1× |
| 4 | ~24 s | 3.8× |
| 8 | ~16 s | 5.6× |

> 💡 For HDDs, use `--workers 2‑4` to avoid excessive seeking.

---

## 🧯 Error Resilience

- ⏳ Timeout per file based on size, with automatic retry.
- 🚫 Corrupted files are skipped gracefully.
- 🧷 Partial writes leave a recoverable temporary file.
- 🚫 No files are ever overwritten.

---

## 📄 Example Report

```
======================================================================
 PROCESSING COMPLETE
======================================================================
 Mode              : APPLY
 Target            : /mnt/media
 Total files       : 3678
 Processed         : 3678
 Renamed           : 3651
 Metadata written  : 3677
 Metadata failures : 1
 Fallback (mtime)  : 106
 Duplicates found  : 0
 Errors            : 2
 Total time        : 06:32
======================================================================
```

---

## 🤝 Contributing

Contributions, issues, and feature requests are welcome!  
Feel free to open a PR or an issue.

---

## 📜 License

MIT License – see `LICENSE` for details.

---

## 🙏 Acknowledgments

- [ExifTool](https://exiftool.org) for powerful metadata handling.
- `xxhash` and `tqdm` for speed and beautiful progress bars.
- The open‑source community.

> Made with ❤️ for permanent, organised memories.
