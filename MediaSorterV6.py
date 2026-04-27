#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MediaSorter V6 – Extremely fast media renaming & metadata tagging.
Reads all metadata in a single exiftool call, writes tags via persistent exiftool,
uses zero content hashing, handles timestamp collisions with a simple counter.
Fully thread‑safe by design – sequential operations avoid race conditions.
"""

import argparse
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

# ----------------------------------------------------------------------
class ExifToolPersistent:
    """Manages a single long‑running exiftool process for fast batch writing."""

    def __init__(self):
        self.proc = subprocess.Popen(
            ['exiftool', '-stay_open', 'True', '-@', '-'],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        # Consume initial ready message
        self._read_until_ready()

    def _read_until_ready(self) -> str:
        """Read output lines until the '{ready}' marker, return all output."""
        lines = []
        while True:
            line = self.proc.stdout.readline()
            if not line:
                break
            lines.append(line)
            if line.strip() == '{ready}':
                break
        return ''.join(lines)

    def execute(self, *args) -> Tuple[bool, str]:
        """
        Send a command to exiftool and wait for execution.
        Returns (success, output_text).
        """
        cmd = ' '.join(args) + '\n-execute\n'
        self.proc.stdin.write(cmd)
        self.proc.stdin.flush()
        output = self._read_until_ready()
        # Simple heuristic: no "Error" and "image files updated" present
        success = ('error' not in output.lower() and
                   ('image files updated' in output.lower() or
                    '1 image files updated' in output.lower() or
                    '0 image files updated' in output.lower()))
        return success, output

    def close(self):
        """Gracefully shut down the persistent process."""
        try:
            self.proc.stdin.write('-stay_open\nFalse\n')
            self.proc.stdin.flush()
            self.proc.communicate(timeout=5)
        except Exception:
            self.proc.kill()


class MediaSorterV6:
    """Organises media files – fast. No hash, no duplicate detection."""

    IMAGE_EXT = {'.jpg', '.jpeg', '.png', '.webp', '.heic', '.heif', '.tiff', '.tif'}
    VIDEO_EXT = {'.mp4', '.mkv', '.mov', '.webm', '.avi', '.m4v', '.3gp', '.wmv', '.flv'}
    UNSUPPORTED_WRITE_EXT = {'.avi'}  # still avoid writing to AVI

    MARKER = "SORTED_BY_MEDIA_SORTER_V2"

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
    ):
        self.target_dir = target_dir.resolve()
        self.label = self._sanitize_label(source_label)
        self.apply = apply
        self.verbose = verbose
        self.skip_processed = skip_processed
        self.force_metadata = force_metadata
        self.date_folder = date_folder
        self.require_metadata = require_metadata

        # Statistics
        self.stats = {
            "total": 0, "processed": 0, "skipped": 0,
            "renamed": 0, "metadata_written": 0, "metadata_failed": 0,
            "metadata_unsupported": 0, "fallback_timestamp": 0,
            "require_metadata_aborts": 0,  # count aborts due to require_metadata
        }
        self.errors: List[str] = []

        # For collision handling – maps base stem -> next suffix number
        self._stem_counter: Dict[str, int] = defaultdict(int)

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
            return (1970 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31 and
                    0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= second <= 59)
        except ValueError:
            return False

    # ------------------------------------------------------------------
    def _scan_files(self) -> List[Path]:
        """Return list of media files (non‑recursive)."""
        files = []
        for entry in os.scandir(self.target_dir):
            if not entry.is_file() or entry.name.startswith('.'):
                continue
            ext = Path(entry.name).suffix.lower()
            if ext in self.IMAGE_EXT or ext in self.VIDEO_EXT:
                files.append(Path(entry.path))
        return files

    def _read_all_metadata(self, file_list: List[Path]) -> Dict[Path, dict]:
        """
        Call exiftool once for batches of files to avoid argument length limits.
        Returns dict mapping Path -> metadata JSON object.
        """
        all_meta = {}
        batch_size = 500  # safe limit for command line
        for i in range(0, len(file_list), batch_size):
            batch = file_list[i:i+batch_size]
            cmd = ['exiftool', '-json', '-G'] + [str(f) for f in batch]
            try:
                proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
                if proc.returncode == 0 and proc.stdout.strip():
                    data = json.loads(proc.stdout)
                    if isinstance(data, list):
                        for entry in data:
                            src = Path(entry['SourceFile'])
                            all_meta[src] = entry
            except Exception as e:
                self.errors.append(f"Metadata read failed for batch: {e}")
        return all_meta

    def _extract_timestamp(self, metadata: dict, file_path: Path) -> Tuple[str, bool]:
        """
        Return (timestamp, is_fallback).
        Tries metadata fields, then mtime.
        """
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
                    return ts, False

        # Fallback to file modification time
        mtime = file_path.stat().st_mtime
        ts = time.strftime('%Y%m%d%H%M%S', time.localtime(mtime))
        return ts, True

    def _is_already_processed(self, metadata: dict) -> bool:
        """Check for our marker in any comment field."""
        comment = (metadata.get('EXIF:Comment') or
                   metadata.get('Comment') or
                   metadata.get('QuickTime:Comment') or '')
        return self.MARKER in comment

    # ------------------------------------------------------------------
    def _plan_file(self, file_path: Path, metadata: dict) -> Optional[dict]:
        """
        Determine new path and whether metadata write is needed.
        Returns an operation dict, or None if skipped.
        """
        # Already processed?
        is_processed = self._is_already_processed(metadata)
        if self.skip_processed and is_processed:
            self.stats["skipped"] += 1
            return None

        # Extract timestamp
        ts, is_fallback = self._extract_timestamp(metadata, file_path)
        if is_fallback:
            self.stats["fallback_timestamp"] += 1

        date_part = ts[:8]
        time_part = ts[8:]

        # Destination directory
        if self.date_folder:
            year, month = date_part[:4], date_part[4:6]
            dest_dir = self.target_dir / year / month
        else:
            dest_dir = self.target_dir

        # Build base stem
        stem = f"{date_part}_{time_part}__{self.label}"

        # Handle collisions (same timestamp)
        counter = self._stem_counter[stem]
        if counter == 0:
            final_stem = stem
        else:
            final_stem = f"{stem}_{counter}"
        self._stem_counter[stem] += 1

        ext = file_path.suffix.lower()
        new_name = f"{final_stem}{ext}"
        new_path = dest_dir / new_name

        # Determine if we need to write metadata
        needs_write = (not is_processed) or self.force_metadata

        op = {
            "src": file_path,
            "dst": new_path,
            "ts_formatted": f"{ts[:4]}:{ts[4:6]}:{ts[6:8]} {ts[8:10]}:{ts[10:12]}:{ts[12:14]}",
            "needs_write": needs_write,
            "is_processed": is_processed,
            "ext": ext,
        }
        return op

    def _write_metadata_for_file(self, op: dict, et: ExifToolPersistent) -> bool:
        """
        Write timestamp and marker using the persistent exiftool process.
        Returns True on success, False on failure (or unsupported).
        """
        src = op["src"]
        ts = op["ts_formatted"]
        ext = op["ext"]

        if src.suffix.lower() in self.UNSUPPORTED_WRITE_EXT:
            self.stats["metadata_unsupported"] += 1
            self.errors.append(f"Unsupported format (static): {src.name}")
            return False

        tags = []
        if ext in self.IMAGE_EXT:
            tags.extend([
                f'-EXIF:DateTimeOriginal={ts}',
                f'-EXIF:CreateDate={ts}',
                f'-EXIF:Comment={self.MARKER}',
            ])
        else:
            tags.extend([
                f'-QuickTime:CreateDate={ts}',
                f'-Comment={self.MARKER}',
            ])
        tags.extend(['-m', '-overwrite_original', '-P'])

        args = tags + [str(src)]
        success, output = et.execute(*args)

        if success:
            self.stats["metadata_written"] += 1
            return True
        else:
            # Check if unsupported dynamically
            if "Can't currently write" in output:
                self.stats["metadata_unsupported"] += 1
                self.errors.append(f"Unsupported format: {src.name}")
            else:
                self.stats["metadata_failed"] += 1
                self.errors.append(f"Metadata write failed for {src.name}: {output.strip()[:200]}")
            return False

    def _apply_rename(self, src: Path, dst: Path) -> bool:
        """Rename file, creating folders if needed."""
        if src == dst:
            return True  # no change needed
        if not self.apply:
            return True  # dry run
        dst.parent.mkdir(parents=True, exist_ok=True)
        try:
            src.rename(dst)
            return True
        except Exception as e:
            self.errors.append(f"Rename failed {src.name} -> {dst.name}: {e}")
            return False

    # ------------------------------------------------------------------
    def run(self):
        start_time = time.time()

        # 1. Scan files
        files = self._scan_files()
        self.stats["total"] = len(files)
        if not files:
            print("No media files found.")
            return
        if self.verbose:
            print(f"Found {self.stats['total']} media files.\n")

        # 2. Bulk metadata extraction
        all_meta = self._read_all_metadata(files)

        # 3. Plan operations
        operations = []
        for f in files:
            meta = all_meta.get(f, {})
            op = self._plan_file(f, meta)
            if op:
                operations.append(op)

        # 4. Execute (apply mode) or dry-run
        if self.apply:
            # Open one persistent exiftool for writes
            et = ExifToolPersistent()
            try:
                # We use a simple progress bar if tqdm is available
                iterator = tqdm(operations, desc="Processing", unit="file") if HAS_TQDM else operations
                for op in iterator:
                    # Write metadata (if needed and format supports it)
                    write_ok = True
                    if op["needs_write"]:
                        write_ok = self._write_metadata_for_file(op, et)

                    if not write_ok and self.require_metadata:
                        self.stats["require_metadata_aborts"] += 1
                        continue  # skip rename

                    # Rename
                    renamed = self._apply_rename(op["src"], op["dst"])
                    if renamed:
                        self.stats["renamed"] += 1
                    self.stats["processed"] += 1
            finally:
                et.close()
        else:
            # Dry-run: just show what would happen
            print("DRY-RUN MODE – No changes will be made.\n")
            for op in operations:
                print(f"  {op['src'].name}  ->  {op['dst']}" +
                      (f"  [write metadata]" if op['needs_write'] else ""))
            print(f"\nWould process {len(operations)} files.")
            self.stats["processed"] = len(operations)

        self.print_report(start_time)

    def print_report(self, start_time: Optional[float] = None):
        s = self.stats
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
        print(f" Aborted (req.meta): {s['require_metadata_aborts']}")
        print(f" Errors            : {len(self.errors)}")
        if start_time:
            elapsed = time.time() - start_time
            print(f" Total time        : {self._format_time(elapsed)}")
        if self.errors:
            print(f"\n First 5 errors:")
            for e in self.errors[:5]:
                print(f"   {e}")
        print("=" * 70)

    def save_json_report(self, path: Path):
        report = {
            "target": str(self.target_dir),
            "mode": "apply" if self.apply else "dry-run",
            "stats": self.stats,
            "errors": self.errors,
        }
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)

    @staticmethod
    def _format_time(seconds: float) -> str:
        h = int(seconds // 3600)
        m = int((seconds % 3600) // 60)
        s = int(seconds % 60)
        if h:
            return f"{h:02d}:{m:02d}:{s:02d}"
        return f"{m:02d}:{s:02d}"


def main():
    parser = argparse.ArgumentParser(description="MediaSorter V6 – ultra-fast media organizer")
    parser.add_argument('--target', required=True, help='Directory with media files')
    parser.add_argument('--label', default='Media', help='Label in filename')
    parser.add_argument('--apply', action='store_true', help='Actually perform operations')
    parser.add_argument('--verbose', action='store_true', help='Verbose output')
    parser.add_argument('--skip-processed', action='store_true', help='Skip files already processed')
    parser.add_argument('--force-metadata', action='store_true', help='Rewrite metadata even if marker exists')
    parser.add_argument('--date-folder', action='store_true', help='Sort into YYYY/MM subfolders')
    parser.add_argument('--require-metadata', action='store_true', help='Abort rename if metadata write fails')
    parser.add_argument('--json-report', help='Save JSON report to file')

    args = parser.parse_args()
    target = Path(args.target)
    if not target.is_dir():
        print(f"Error: '{args.target}' is not a directory.", file=sys.stderr)
        sys.exit(1)

    MediaSorterV6.check_dependencies()

    if not args.apply:
        print("=" * 70)
        print(" DRY-RUN MODE – No changes will be made")
        print(" Use --apply to actually perform operations")
        print("=" * 70)

    sorter = MediaSorterV6(
        target_dir=target,
        source_label=args.label,
        apply=args.apply,
        verbose=args.verbose,
        skip_processed=args.skip_processed,
        force_metadata=args.force_metadata,
        date_folder=args.date_folder,
        require_metadata=args.require_metadata,
    )
    sorter.run()

    if args.json_report:
        sorter.save_json_report(Path(args.json_report))
        print(f"\nJSON report saved to {args.json_report}")

    sys.exit(0 if not sorter.errors else 1)


if __name__ == '__main__':
    main()
