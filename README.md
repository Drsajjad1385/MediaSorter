# MediaSorter V6

**Ultra‑fast media organizer & deduplicator**  

Rename, sort, and tag thousands of photos and videos in seconds.  
Permanently write capture timestamps into your files, organize them into date folders,  
and optionally detect exact duplicates — all with minimal disk I/O and maximum safety.

<p align="center">
  <img src="https://img.shields.io/badge/version-6.0-blue" alt="version">
  <img src="https://img.shields.io/badge/platform-linux%20%7C%20macos%20%7C%20windows-lightgrey" alt="platform">
  <img src="https://img.shields.io/badge/requires-exiftool-orange" alt="requires exiftool">
</p>

---

## 🚀 Why MediaSorter V6?

Most media organizers are slow because they spawn a separate `exiftool` process for **every single file**.  
For a library of 10,000 photos that means **20,000+ process launches** – your CPU and disk spend more time on overhead than on actual work.

**MediaSorter V6 solves this with a radical two‑script approach:**

| Script | Purpose | Time for 10K files (real‑world) |
|--------|---------|----------------------------------|
| `MediaSorterV6.py` | **Sort & tag** (rename, write metadata) | **< 20 seconds** |
| `MediaDeduper.py`  | **Duplicate detection** (content‑based) | **5–30 seconds** (if needed) |

No more waiting minutes for a simple rename. Run the sorter daily, and let the deduper find duplicates only when you need it.

---

## ⚡ Performance Secrets

- **Bulk metadata extraction** – all files are read in **one** (or a few) `exiftool -json` call.
- **Persistent `exiftool` instance** – for writing tags a single long‑running process is reused, eliminating startup overhead.
- **Zero content hashing during sorting** – the old UID hashing is completely removed. Timestamp collisions are resolved with a **fast in‑memory counter**.
- **Smart 3‑stage deduplication** (in the Deduper) – file size → partial hash (4 KiB) → full hash ensures only **very few** total file reads.
- **Optional `xxHash`** – if installed, hashing becomes even faster.

**Hardware‑friendly:** Designed to keep disk I/O minimal, so even large spinning drives stay fast.

---

## 📦 What’s Included

```
.
├── MediaSorterV6.py   ← main ultra-fast sorter
├── MediaDeduper.py    ← separate duplicate finder / handler
└── README.md
```

---

## 🧰 System Requirements

- **Python 3.8+**
- **ExifTool** (by Phil Harvey) – [install from here](https://exiftool.org)
- Optional but recommended: [`xxhash`](https://pypi.org/project/xxhash/) for faster hashing  
  `pip install xxhash`
- Optional: [`tqdm`](https://pypi.org/project/tqdm/) for a progress bar  
  `pip install tqdm`

---

## 🚀 Quick Start: Sort & Tag (the daily driver)

### Dry‑run (see what will happen)

```bash
python MediaSorterV6.py \
    --target /path/to/photos \
    --label "Vacation2025" \
    --date-folder \
    --verbose
```

### Apply (actually rename and tag)

```bash
python MediaSorterV6.py \
    --target /path/to/photos \
    --apply \
    --label "Vacation2025" \
    --date-folder
```

**Results:**
- Files renamed to: `20250715_143025__Vacation2025.jpg`
- Automatically placed into `YYYY/MM` subfolders if `--date-folder` is used.
- Timestamp written into `EXIF:DateTimeOriginal`, `EXIF:CreateDate` (or `QuickTime:CreateDate` for videos).
- Marker comment (`SORTED_BY_MEDIA_SORTER_V2`) added so future runs can skip already processed files.

### Example output

```
Found 8423 media files.

Processing: 100%|█████████████████| 8423/8423 [00:07<00:00, 1120.45file/s]

 PROCESSING COMPLETE
======================================================================
 Mode              : APPLY
 Target            : /mnt/nas/photos
 Total files       : 8423
 Processed         : 8423
 Skipped (already) : 0
 Renamed           : 8421
 Metadata written  : 8421
 Metadata failed   : 0
 Metadata unsupported: 2
 Fallback (mtime)  : 0
 Aborted (req.meta): 0
 Errors            : 0
 Total time        : 00:07
======================================================================
```

---

## 🔍 Duplicate Detection (when you need it)

Run the deduper **separately** – you probably only need this monthly or before archival.

### Just report

```bash
python MediaDeduper.py --target /path/to/photos --recursive
```

### Move duplicates to a folder

```bash
python MediaDeduper.py --target /path/to/photos \
    --recursive --apply --action move --dest /path/to/duplicates
```

### Delete duplicates *(careful!)*

```bash
python MediaDeduper.py --target /path/to/photos \
    --recursive --apply --action delete
```

**How it works:**  
1. Groups files by **exact size** → 2. Checks **first 4 KiB** → 3. **Full content hash** on remaining candidates.  
   No false positives, no wasted I/O.

---

## 🛠️ Full Options Reference

### `MediaSorterV6.py`

| Argument | Type | Description |
|----------|------|-------------|
| `--target` | Path | **Required** – Directory with media files |
| `--apply` | Flag | Actually rename & write metadata (default: dry‑run) |
| `--label` | String | Label inserted into filename (default: `Media`) |
| `--date-folder` | Flag | Sort files into `YYYY/MM` subdirectories |
| `--skip-processed` | Flag | Ignore files already tagged by this script |
| `--force-metadata` | Flag | Rewrite metadata even if marker exists |
| `--require-metadata` | Flag | Abort rename if metadata write fails |
| `--verbose` | Flag | Print detailed per‑file info |
| `--json-report` | Path | Save a JSON summary of the run |

### `MediaDeduper.py`

| Argument | Type | Description |
|----------|------|-------------|
| `--target` | Path | **Required** – Directory to scan |
| `--recursive` | Flag | Include subdirectories |
| `--apply` | Flag | Actually perform the action (default: report only) |
| `--action` | Choice | `report`, `move`, or `delete` |
| `--dest` | Path | Destination folder (for `move` action) |

---

## 🧠 Design Decisions & Safety

- **No content hashing during rename** → massive speed boost.
- **Time‑stamp collisions** (two photos taken in the same second) are resolved with a **simple increment** (e.g., `_1`, `_2`) rather than a fragile UID.
- **Marker comment** prevents re‑processing, but `--force-metadata` allows refreshing tags.
- **Orphaned temp‑file recovery** – if a write is interrupted, the script automatically restores the original file.
- **Dry‑run by default** – no accidental changes until you add `--apply`.
- **Thread‑safe by design** – sorting runs sequentially (after gathering all metadata) so no race conditions exist.

---

## 📈 Upgrading from V5

V6 is a complete rewrite focused on speed and simplicity.  
Key differences:

- **UID removed** – no more hashing, no more 8‑char hex in filenames.
- **Two scripts** instead of one monolithic tool.
- `--workers` argument removed (no longer needed – everything is so fast it runs single‑threaded).
- Much simpler code, easier to maintain and extend.

---

## 🔧 Installation

1. Install [ExifTool](https://exiftool.org) and make sure it’s in your `PATH`.
2. (Optional) `pip install tqdm xxhash`
3. Download `MediaSorterV6.py` and `MediaDeduper.py` into your project.
4. Run the commands above.

---

## 🤝 Contributing

Found a bug? Have a feature idea?  
Pull requests are welcome. Keep the core philosophy in mind: **extreme speed + separate deduplication**.

---

## 📜 License

MIT – Do what you want, just keep the attribution.

---

<p align="center">
  <b>Enjoy your perfectly organised media library 🎉</b>
</p>
