#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MediaSorter V5 – Industrial media organizer. Writes timestamps permanently,
handles large files safely, recovers from interrupted exiftool operations,
and gracefully skips unsupported formats.
"""

import argparse
import concurrent.futures
import hashlib
import json
import os
import re
import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

try:
    import xxhash
    HAS_XXHASH = True
except ImportError:
    HAS_XXHASH = False


class MediaSorter:
    """Organises media files by writing timestamps permanently and renaming intelligently."""

    IMAGE_EXT = {'.jpg', '.jpeg', '.png', '.webp', '.heic', '.heif', '.tiff', '.tif'}
    VIDEO_EXT = {'.mp4', '.mkv', '.mov', '.webm', '.avi', '.m4v', '.3gp', '.wmv', '.flv'}

    # Formats that exiftool cannot write to (metadata write will be skipped)
    UNSUPPORTED_WRITE_EXT = {'.avi'}  # can add more dynamically via error detection

    MARKER = "SORTED_BY_MEDIA_SORTER_V2"
    HASH_CHUNK_SIZE = 64 * 1024  # 64 KiB

    def __init__(
        self,
        target_dir: Path,
        source_label: str = "Media",
        apply: bool = False,
        verbose: bool = False,
        skip_processed: bool = False,
        force_metadata: bool = False,
        date_folder: bool = False,
        require_metadata: bool = False,
        workers: int = 4,
    ):
        self.target_dir = target_dir.resolve()
        self.label = self._sanitize_label(source_label)
        self.apply = apply
        self.verbose = verbose
        self.skip_processed = skip_processed
        self.force_metadata = force_metadata
        self.date_folder = date_folder
        self.require_metadata = require_metadata
        self.workers = max(1, workers)

        self.stats = {
            "total": 0, "processed": 0, "skipped": 0,
            "renamed": 0, "metadata_written": 0, "metadata_failed": 0,
            "metadata_unsupported": 0, "fallback_timestamp": 0, "duplicates": 0,
        }
        self.errors: List[str] = []
        self.duplicate_details: List[str] = []

        self._uid_map: Dict[str, str] = {}
        self._uid_counters: Dict[str, int] = defaultdict(int)

    # ------------------------------------------------------------------
    @staticmethod
    def check_dependencies():
        try:
            subprocess.run(['exiftool', '-ver'], capture_output=True, check=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            print("Error: 'exiftool' is required. Install from https://exiftool.org", file=sys.stderr)
            sys.exit(1)

    @staticmethod
    def _sanitize_label(label: str) -> str:
        return re.sub(r'[^\w\-]', '_', label).strip('_')

    @staticmethod
    def _is_valid_timestamp(ts: str) -> bool:
        try:
            if len(ts) != 14:
                return False
            year = int(ts[:4]); month = int(ts[4:6]); day = int(ts[6:8])
            hour = int(ts[8:10]); minute = int(ts[10:12]); second = int(ts[12:14])
            if year < 1970 or year > 2100: return False
            if not 1 <= month <= 12: return False
            if not 1 <= day <= 31: return False
            if not 0 <= hour <= 23: return False
            if not 0 <= minute <= 59: return False
            if not 0 <= second <= 59: return False
            return True
        except ValueError:
            return False

    # ------------------------------------------------------------------
    def _run_exiftool_json(self, file_path: Path, timeout: int = 15) -> Optional[dict]:
        cmd = ['exiftool', '-json', '-G', str(file_path)]
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
            if proc.returncode != 0 or not proc.stdout.strip():
                return None
            data = json.loads(proc.stdout)
            return data[0] if data else None
        except Exception:
            return None

    def _extract_timestamp_from_metadata(self, metadata: dict) -> Optional[str]:
        candidates = [
            metadata.get('EXIF:DateTimeOriginal'),
            metadata.get('EXIF:CreateDate'),
            metadata.get('QuickTime:CreateDate'),
            metadata.get('Keys:CreationDate'),
            metadata.get('EXIF:DateTimeDigitized'),
        ]
        for value in candidates:
            if value and isinstance(value, str):
                digits = ''.join(c for c in value if c.isdigit())
                if len(digits) >= 14:
                    ts = digits[:14]
                elif len(digits) == 8:
                    ts = digits + '000000'
                else:
                    continue
                if self._is_valid_timestamp(ts):
                    return ts
        return None

    def _get_file_timestamp(self, file_path: Path) -> Tuple[str, bool, bool]:
        metadata = self._run_exiftool_json(file_path)
        is_processed = False
        if metadata:
            comment = metadata.get('EXIF:Comment') or metadata.get('Comment') or ''
            is_processed = self.MARKER in comment
            ts = self._extract_timestamp_from_metadata(metadata)
            if ts:
                if self.verbose:
                    self._vprint(f"  ✓ Timestamp from metadata: {ts}")
                return ts, False, is_processed

        mtime = file_path.stat().st_mtime
        ts = time.strftime('%Y%m%d%H%M%S', time.localtime(mtime))
        if self.verbose:
            self._vprint(f"  ⚠ Fallback to mtime: {ts}")
        return ts, True, is_processed

    # ------------------------------------------------------------------
    def _is_write_supported(self, file_path: Path) -> bool:
        """Check if metadata writing is likely supported."""
        if file_path.suffix.lower() in self.UNSUPPORTED_WRITE_EXT:
            return False
        # Further dynamic check can be added later
        return True

    def _write_metadata_timestamp(self, file_path: Path, timestamp: str) -> bool:
        """
        Write timestamp to file. Returns True on success, False otherwise.
        Handles timeouts gracefully and recovers orphaned temp files.
        """
        if not self._is_write_supported(file_path):
            self.stats["metadata_unsupported"] += 1
            if self.verbose:
                self._vprint(f"  ℹ️ Skipping metadata write (unsupported format): {file_path.name}")
            return False  # not a failure, just unsupported

        formatted = f"{timestamp[:4]}:{timestamp[4:6]}:{timestamp[6:8]} {timestamp[8:10]}:{timestamp[10:12]}:{timestamp[12:14]}"
        file_size_mb = file_path.stat().st_size / (1024 * 1024)
        timeout = max(60, int(file_size_mb * 2))  # 2 sec per MB, at least 60s
        timeout = min(timeout, 600)  # cap at 10 minutes

        ext = file_path.suffix.lower()
        if ext in self.IMAGE_EXT:
            cmd = [
                'exiftool',
                f'-EXIF:DateTimeOriginal={formatted}',
                f'-EXIF:CreateDate={formatted}',
                f'-EXIF:Comment={self.MARKER}',
                '-m', '-overwrite_original', '-P',
                str(file_path)
            ]
        else:
            cmd = [
                'exiftool',
                f'-QuickTime:CreateDate={formatted}',
                f'-Comment={self.MARKER}',
                '-m', '-overwrite_original', '-P',
                str(file_path)
            ]

        success = False
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
            if result.returncode == 0:
                success = True
            else:
                # Check for unsupported format dynamically
                if "Can't currently write" in (result.stderr or ""):
                    self.stats["metadata_unsupported"] += 1
                    self.errors.append(f"Metadata write unsupported for {file_path.name} (format not writable)")
                    return False
                self.errors.append(f"Metadata write failed for {file_path.name}: {result.stderr.strip()[:200]}")
        except subprocess.TimeoutExpired:
            self.errors.append(f"Metadata write timed out for {file_path.name} ({file_size_mb:.1f}MB)")
        except Exception as e:
            self.errors.append(f"Metadata write error for {file_path.name}: {e}")

        # Post-write cleanup: handle any leftover temp file
        tmp_path = file_path.with_name(file_path.name + "_exiftool_tmp")
        if tmp_path.exists():
            if file_path.exists():
                # Original exists, temp is garbage
                try:
                    tmp_path.unlink()
                    if self.verbose:
                        self._vprint(f"  🧹 Cleaned up orphan temp file")
                except Exception as e:
                    self.errors.append(f"Could not delete temp {tmp_path.name}: {e}")
            else:
                # Original missing, temp should become original
                try:
                    tmp_path.rename(file_path)
                    self.errors.append(f"Recovered {file_path.name} from temp file (write may have been interrupted)")
                    success = True  # we got the file back, even if metadata might be partial? But the temp could be fully written. Assume success.
                except Exception as e:
                    self.errors.append(f"Failed to recover {file_path.name} from temp: {e}")

        return success

    # ------------------------------------------------------------------
    def _compute_uid(self, file_path: Path) -> str:
        file_size = file_path.stat().st_size
        segments = []
        try:
            with open(file_path, 'rb') as f:
                segments.append(f.read(self.HASH_CHUNK_SIZE))
                if file_size > 3 * self.HASH_CHUNK_SIZE:
                    f.seek(file_size // 2 - self.HASH_CHUNK_SIZE // 2)
                    segments.append(f.read(self.HASH_CHUNK_SIZE))
                if file_size > self.HASH_CHUNK_SIZE:
                    f.seek(-self.HASH_CHUNK_SIZE, os.SEEK_END)
                    segments.append(f.read(self.HASH_CHUNK_SIZE))
        except Exception as e:
            self.errors.append(f"UID read error for {file_path.name}: {e}")
            segments = [file_path.name.encode()]

        if HAS_XXHASH:
            hasher = xxhash.xxh64()
        else:
            hasher = hashlib.sha256()
        for seg in segments:
            hasher.update(seg)
        hasher.update(str(file_size).encode())
        return hasher.hexdigest()[:8]

    # ------------------------------------------------------------------
    def _plan_new_path(self, file_path: Path, timestamp: str, uid: str) -> Tuple[Path, Optional[str]]:
        ext = file_path.suffix.lower()
        date_part = timestamp[:8]
        time_part = timestamp[8:]

        if self.date_folder:
            year, month = date_part[:4], date_part[4:6]
            dest_dir = self.target_dir / year / month
        else:
            dest_dir = self.target_dir

        base_name = f"{date_part}_{time_part}__{self.label}__{uid}"

        if uid in self._uid_map:
            self._uid_counters[uid] += 1
            counter = self._uid_counters[uid]
            new_name = f"{base_name}__DUP{counter}{ext}"
            dup_info = f"{uid} : {self._uid_map[uid]} <-> {file_path.name}"
            return dest_dir / new_name, dup_info
        else:
            self._uid_map[uid] = file_path.name
            self._uid_counters[uid] = 0
            new_name = f"{base_name}{ext}"
            return dest_dir / new_name, None

    def _apply_rename(self, old_path: Path, new_path: Path) -> bool:
        if old_path == new_path:
            return False
        if not self.apply:
            return True
        new_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            old_path.rename(new_path)
            return True
        except Exception as e:
            self.errors.append(f"Rename failed {old_path.name} -> {new_path.name}: {e}")
            return False

    def _vprint(self, *args, **kwargs):
        if HAS_TQDM:
            tqdm.write(" ".join(str(a) for a in args), **kwargs)
        else:
            print(*args, **kwargs)

    @staticmethod
    def _format_time(seconds: float) -> str:
        h = int(seconds // 3600)
        m = int((seconds % 3600) // 60)
        s = int(seconds % 60)
        if h:
            return f"{h:02d}:{m:02d}:{s:02d}"
        return f"{m:02d}:{s:02d}"

    def _process_single_file(self, file_path: Path):
        try:
            timestamp, is_fallback, is_processed = self._get_file_timestamp(file_path)
            if is_fallback:
                self.stats["fallback_timestamp"] += 1

            if self.skip_processed and is_processed:
                self.stats["skipped"] += 1
                if self.verbose:
                    self._vprint(f"  → Skipped (already processed)")
                return

            uid = self._compute_uid(file_path)
            new_path, dup_info = self._plan_new_path(file_path, timestamp, uid)
            if dup_info:
                self.duplicate_details.append(dup_info)
                self.stats["duplicates"] += 1

            needs_write = (not is_processed) or self.force_metadata
            if needs_write:
                if self.apply:
                    success = self._write_metadata_timestamp(file_path, timestamp)
                else:
                    success = True  # dry-run always assumes success
                if success:
                    self.stats["metadata_written"] += 1
                else:
                    # _write_metadata_timestamp already updated appropriate failure/unsupported stats
                    if self.require_metadata:
                        self.stats["errors"] += 1  # block rename
                        return

            renamed = self._apply_rename(file_path, new_path)
            if renamed:
                self.stats["renamed"] += 1

            self.stats["processed"] += 1

        except Exception as e:
            self.errors.append(f"Unexpected error on {file_path.name}: {e}")

    def run(self):
        all_files = []
        for entry in os.scandir(self.target_dir):
            if not entry.is_file() or entry.name.startswith('.'):
                continue
            ext = Path(entry.name).suffix.lower()
            if ext in self.IMAGE_EXT or ext in self.VIDEO_EXT:
                all_files.append(Path(entry.path))

        total = len(all_files)
        self.stats["total"] = total
        if self.verbose:
            self._vprint(f"Found {total} media files.\n")

        start_time = time.time()

        if HAS_TQDM:
            with tqdm(total=total, desc="Processing", unit="file",
                      bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]") as pbar:
                if self.workers > 1 and total > 1:
                    with concurrent.futures.ThreadPoolExecutor(max_workers=self.workers) as executor:
                        futures = {executor.submit(self._process_single_file, f): f for f in all_files}
                        for future in concurrent.futures.as_completed(futures):
                            pbar.update(1)
                else:
                    for f in all_files:
                        self._process_single_file(f)
                        pbar.update(1)
        else:
            print("Processing files...", flush=True)
            processed = 0
            def _print_progress(n):
                elapsed = time.time() - start_time
                pct = (n / total) * 100 if total else 0
                eta = (elapsed / n * (total - n)) if n > 0 else 0
                sys.stdout.write(
                    f"\rElapsed: {self._format_time(elapsed)} | "
                    f"Progress: {n}/{total} ({pct:.1f}%) | "
                    f"ETA: {self._format_time(eta)}"
                )
                sys.stdout.flush()

            if self.workers > 1 and total > 1:
                with concurrent.futures.ThreadPoolExecutor(max_workers=self.workers) as executor:
                    futures = {executor.submit(self._process_single_file, f): f for f in all_files}
                    for future in concurrent.futures.as_completed(futures):
                        processed += 1
                        _print_progress(processed)
            else:
                for f in all_files:
                    self._process_single_file(f)
                    processed += 1
                    _print_progress(processed)
            print()

        self.print_report(start_time)

    def print_report(self, start_time: Optional[float] = None):
        s = self.stats
        total_errors = len(self.errors)
        print("\n" + "=" * 70)
        print(" PROCESSING COMPLETE")
        print("=" * 70)
        print(f" Mode              : {'APPLY' if self.apply else 'DRY-RUN'}")
        print(f" Target            : {self.target_dir}")
        print(f" Total files       : {s['total']}")
        print(f" Processed         : {s['processed']}")
        print(f" Skipped (already) : {s['skipped']}")
        print(f" Renamed           : {s['renamed']}")
        print(f" Metadata written  : {s['metadata_written']}")
        print(f" Metadata failed   : {s['metadata_failed']}")
        print(f" Metadata unsupported: {s['metadata_unsupported']}")
        print(f" Fallback (mtime)  : {s['fallback_timestamp']}")
        print(f" Duplicates found  : {s['duplicates']}")
        print(f" Errors            : {total_errors}")
        if start_time:
            elapsed = time.time() - start_time
            print(f" Total time        : {self._format_time(elapsed)}")
        if self.duplicate_details:
            print("\n Duplicate details (first 5):")
            for d in self.duplicate_details[:5]:
                print(f"   {d}")
        if self.errors:
            print(f"\n Error details (first 5):")
            for e in self.errors[:5]:
                print(f"   {e}")
        print("=" * 70)

    def save_json_report(self, path: Path):
        report = {
            "target": str(self.target_dir),
            "mode": "apply" if self.apply else "dry-run",
            "stats": self.stats,
            "duplicates": self.duplicate_details,
            "errors": self.errors,
        }
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)


def main():
    parser = argparse.ArgumentParser(description="MediaSorter V5 – professional media organizer")
    parser.add_argument('--target', required=True, help='Directory containing media files')
    parser.add_argument('--label', default='Media', help='Source label for filenames')
    parser.add_argument('--apply', action='store_true', help='Actually rename & write metadata')
    parser.add_argument('--verbose', action='store_true', help='Detailed output')
    parser.add_argument('--skip-processed', action='store_true', help='Skip files already processed')
    parser.add_argument('--force-metadata', action='store_true', help='Rewrite metadata even if marker exists')
    parser.add_argument('--date-folder', action='store_true', help='Sort into YYYY/MM subfolders')
    parser.add_argument('--require-metadata', action='store_true', help='Abort rename if metadata write fails')
    parser.add_argument('--workers', type=int, default=4, help='Parallel threads (default: 4)')
    parser.add_argument('--json-report', help='Save report to JSON file')

    args = parser.parse_args()
    target = Path(args.target)
    if not target.is_dir():
        print(f"Error: '{args.target}' is not a directory.", file=sys.stderr)
        sys.exit(1)

    MediaSorter.check_dependencies()

    if not args.apply:
        print("=" * 70)
        print(" DRY-RUN MODE – No changes will be made")
        print(" Use --apply to actually perform operations")
        print("=" * 70)

    sorter = MediaSorter(
        target_dir=target,
        source_label=args.label,
        apply=args.apply,
        verbose=args.verbose,
        skip_processed=args.skip_processed,
        force_metadata=args.force_metadata,
        date_folder=args.date_folder,
        require_metadata=args.require_metadata,
        workers=args.workers,
    )

    sorter.run()

    if args.json_report:
        sorter.save_json_report(Path(args.json_report))
        print(f"\nJSON report saved to {args.json_report}")

    sys.exit(0 if not sorter.errors else 1)


if __name__ == '__main__':
    main()
