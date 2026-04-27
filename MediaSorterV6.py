#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MediaSorter V9 – Fast media organizer with LIVE progress during metadata write.
Single exiftool call for read, single call for write with real‑time progress bar.
"""

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False


class SimpleProgress:
    """Fallback progress bar when tqdm is not installed."""

    def __init__(self, total: int, desc: str = "Progress"):
        self.total = total
        self.desc = desc
        self.current = 0
        self.start_time = time.time()
        self._last_len = 0

    def update(self, n: int = 1):
        self.current += n
        self._print()

    def _print(self):
        elapsed = time.time() - self.start_time
        percent = (self.current / self.total) * 100 if self.total else 0
        if self.current > 0:
            eta = (elapsed / self.current) * (self.total - self.current)
        else:
            eta = 0
        elapsed_str = self._fmt(elapsed)
        eta_str = self._fmt(eta)
        bar_len = 30
        filled = int(bar_len * self.current // self.total) if self.total else 0
        bar = '█' * filled + '░' * (bar_len - filled)
        line = f"\r{self.desc}: |{bar}| {self.current}/{self.total} ({percent:.1f}%) [{elapsed_str}<{eta_str}]"
        sys.stdout.write(line.ljust(self._last_len))
        sys.stdout.flush()
        self._last_len = len(line)

    def close(self):
        self._print()
        sys.stdout.write('\n')
        sys.stdout.flush()

    @staticmethod
    def _fmt(seconds: float) -> str:
        if seconds < 0:
            seconds = 0
        m, s = divmod(int(seconds), 60)
        h, m = divmod(m, 60)
        if h:
            return f"{h:02d}:{m:02d}:{s:02d}"
        return f"{m:02d}:{s:02d}"


class MediaSorter:
    """Ultra‑fast media organiser with live progress on metadata write."""

    IMAGE_EXT = {'.jpg', '.jpeg', '.png', '.webp', '.heic', '.heif', '.tiff', '.tif'}
    VIDEO_EXT = {'.mp4', '.mkv', '.mov', '.webm', '.avi', '.m4v', '.3gp', '.wmv', '.flv'}
    UNSUPPORTED_WRITE_EXT = {'.avi'}

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

        self.stats = {
            "total": 0, "processed": 0, "skipped": 0,
            "renamed": 0, "metadata_written": 0, "metadata_failed": 0,
            "metadata_unsupported": 0, "fallback_timestamp": 0,
            "require_metadata_aborts": 0,
        }
        self.errors: List[str] = []
        self._stem_counter: Dict[str, int] = defaultdict(int)

    @staticmethod
    def check_dependencies():
        try:
            subprocess.run(['exiftool', '-ver'], capture_output=True, check=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            print("Error: 'exiftool' required. Install from https://exiftool.org", file=sys.stderr)
            sys.exit(1)

    @staticmethod
    def _sanitize_label(label: str) -> str:
        return re.sub(r'[^\w\-]', '_', label).strip('_')

    @staticmethod
    def _is_valid_timestamp(ts: str) -> bool:
        try:
            if len(ts) != 14:
                return False
            y, m, d, h, mi, s = map(int, [ts[0:4], ts[4:6], ts[6:8],
                                          ts[8:10], ts[10:12], ts[12:14]])
            return (1970 <= y <= 2100 and 1 <= m <= 12 and 1 <= d <= 31 and
                    0 <= h <= 23 and 0 <= mi <= 59 and 0 <= s <= 59)
        except ValueError:
            return False

    # ------------------------------------------------------------------
    def _scan_files(self) -> List[Path]:
        files = []
        for entry in os.scandir(self.target_dir):
            if not entry.is_file() or entry.name.startswith('.'):
                continue
            ext = Path(entry.name).suffix.lower()
            if ext in self.IMAGE_EXT or ext in self.VIDEO_EXT:
                files.append(Path(entry.path))
        return files

    def _read_all_metadata(self, file_list: List[Path]) -> Dict[Path, dict]:
        if not file_list:
            return {}
        fd, tmpname = tempfile.mkstemp(suffix='.txt', prefix='files_')
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                for fp in file_list:
                    f.write(str(fp) + '\n')
            print("Reading metadata in one call...")
            start = time.time()
            proc = subprocess.run(
                ['exiftool', '-json', '-G', '-@', tmpname],
                capture_output=True, text=True, timeout=600
            )
            elapsed = time.time() - start
            if proc.returncode != 0 or not proc.stdout.strip():
                self.errors.append(f"Metadata read failed: {proc.stderr.strip()[:200]}")
                return {}
            data = json.loads(proc.stdout)
            all_meta = {}
            for entry in data:
                src = Path(entry['SourceFile'])
                all_meta[src] = entry
            print(f"  Read metadata in {elapsed:.1f}s")
            return all_meta
        except Exception as e:
            self.errors.append(f"Metadata read error: {e}")
            return {}
        finally:
            try:
                os.unlink(tmpname)
            except OSError:
                pass

    def _extract_timestamp(self, metadata: dict, file_path: Path) -> Tuple[str, bool]:
        fields = ['EXIF:DateTimeOriginal', 'EXIF:CreateDate',
                  'QuickTime:CreateDate', 'Keys:CreationDate',
                  'EXIF:DateTimeDigitized']
        for field in fields:
            val = metadata.get(field)
            if val and isinstance(val, str):
                digits = ''.join(c for c in val if c.isdigit())
                if len(digits) >= 14:
                    ts = digits[:14]
                elif len(digits) == 8:
                    ts = digits + '000000'
                else:
                    continue
                if self._is_valid_timestamp(ts):
                    return ts, False
        mtime = file_path.stat().st_mtime
        ts = time.strftime('%Y%m%d%H%M%S', time.localtime(mtime))
        return ts, True

    def _is_already_processed(self, metadata: dict) -> bool:
        comment = (metadata.get('EXIF:Comment') or
                   metadata.get('Comment') or
                   metadata.get('QuickTime:Comment') or '')
        return self.MARKER in comment

    # ------------------------------------------------------------------
    def _plan_file(self, file_path: Path, metadata: dict) -> Optional[dict]:
        is_processed = self._is_already_processed(metadata)
        if self.skip_processed and is_processed:
            self.stats["skipped"] += 1
            return None

        ts, is_fallback = self._extract_timestamp(metadata, file_path)
        if is_fallback:
            self.stats["fallback_timestamp"] += 1

        date_part = ts[:8]
        time_part = ts[8:]

        if self.date_folder:
            year, month = date_part[:4], date_part[4:6]
            dest_dir = self.target_dir / year / month
        else:
            dest_dir = self.target_dir

        stem = f"{date_part}_{time_part}__{self.label}"
        counter = self._stem_counter[stem]
        final_stem = stem if counter == 0 else f"{stem}_{counter}"
        self._stem_counter[stem] += 1

        ext = file_path.suffix.lower()
        new_path = dest_dir / f"{final_stem}{ext}"

        needs_write = (not is_processed) or self.force_metadata
        return {
            "src": file_path,
            "dst": new_path,
            "ts_formatted": f"{ts[:4]}:{ts[4:6]}:{ts[6:8]} {ts[8:10]}:{ts[10:12]}:{ts[12:14]}",
            "needs_write": needs_write,
            "ext": ext,
            "unsupported_write": ext in self.UNSUPPORTED_WRITE_EXT,
        }

    # ------------------------------------------------------------------
    def _write_metadata_bulk(self, ops: List[dict]) -> bool:
        """
        Writes metadata to all files using one exiftool -@ process.
        Parses stdout in real-time to show live progress.
        """
        writes = [op for op in ops if op["needs_write"] and not op["unsupported_write"]]
        if not writes:
            return True

        fd, tmpname = tempfile.mkstemp(suffix='.txt', prefix='exifargs_')
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as argfile:
                for op in writes:
                    ext = op["ext"]
                    ts = op["ts_formatted"]
                    if ext in self.IMAGE_EXT:
                        lines = [
                            f'-EXIF:DateTimeOriginal={ts}',
                            f'-EXIF:CreateDate={ts}',
                            f'-EXIF:Comment={self.MARKER}',
                        ]
                    else:
                        lines = [
                            f'-QuickTime:CreateDate={ts}',
                            f'-Comment={self.MARKER}',
                        ]
                    lines += ['-m', '-overwrite_original', '-P']
                    for line in lines:
                        argfile.write(line + '\n')
                    argfile.write(str(op["src"]) + '\n')
                    argfile.write('-execute\n')
                argfile.write('-execute\n')

            print(f"Writing metadata for {len(writes)} files...")
            # Start process with Popen to read stdout line by line
            proc = subprocess.Popen(
                ['exiftool', '-@', tmpname],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1
            )

            # Progress bar
            total_writes = len(writes)
            if HAS_TQDM:
                pbar = tqdm(total=total_writes, desc="Writing metadata", unit="file")
            else:
                pbar = SimpleProgress(total_writes, desc="Writing metadata")

            files_updated_count = 0
            success_count = 0
            for line in proc.stdout:
                line = line.strip()
                # ExifTool outputs "1 image files updated" after each -execute block
                if "image files updated" in line:
                    # Increment progress (one file processed)
                    files_updated_count += 1
                    pbar.update(1)
                # Optionally detect errors (but we'll still count as processed)
                # We assume success if the line says "1 image files updated"
                if "1 image files updated" in line:
                    success_count += 1

            proc.wait()
            pbar.close()

            if proc.returncode == 0:
                self.stats["metadata_written"] += success_count
                # Some files may have been updated but not counted? The count should match writes.
                # if we had errors, success_count might be less than total_writes.
                if success_count != total_writes:
                    failed_count = total_writes - success_count
                    self.stats["metadata_failed"] += failed_count
                    self.errors.append(f"Some metadata writes may have failed ({failed_count} not confirmed)")
                return True
            else:
                self.stats["metadata_failed"] += total_writes
                self.errors.append(f"Bulk metadata write error (return code {proc.returncode})")
                for op in writes:
                    op["_write_failed"] = True
                return False
        except Exception as e:
            self.errors.append(f"Metadata write process error: {e}")
            self.stats["metadata_failed"] += len(writes)
            for op in writes:
                op["_write_failed"] = True
            return False
        finally:
            try:
                os.unlink(tmpname)
            except OSError:
                pass

    def _apply_rename(self, src: Path, dst: Path) -> bool:
        if src == dst:
            return True
        if not self.apply:
            return True
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

        # 1. Scan
        files = self._scan_files()
        self.stats["total"] = len(files)
        if not files:
            print("No media files found.")
            return
        print(f"Found {self.stats['total']} media files.\n")

        # 2. Read all metadata
        all_meta = self._read_all_metadata(files)

        # 3. Plan
        print("Planning names...")
        operations = []
        for f in files:
            meta = all_meta.get(f, {})
            op = self._plan_file(f, meta)
            if op:
                operations.append(op)
        print(f"Planned {len(operations)} operations.\n")

        # 4. Execute
        if self.apply:
            # Bulk write metadata (with live progress)
            self._write_metadata_bulk(operations)

            # Rename with live progress
            print("Renaming files...")
            if HAS_TQDM:
                pbar = tqdm(operations, desc="Renaming", unit="file")
            else:
                pbar = SimpleProgress(len(operations), desc="Renaming")

            for op in operations:
                if op.get("_write_failed") and self.require_metadata:
                    self.stats["require_metadata_aborts"] += 1
                    self.stats["processed"] += 1
                    pbar.update(1)
                    continue
                renamed = self._apply_rename(op["src"], op["dst"])
                if renamed:
                    self.stats["renamed"] += 1
                self.stats["processed"] += 1
                pbar.update(1)

            pbar.close()
        else:
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
    parser = argparse.ArgumentParser(description="MediaSorter V9 – ultra‑fast with live write progress")
    parser.add_argument('--target', required=True, help='Directory with media files')
    parser.add_argument('--label', default='Media', help='Label in filename')
    parser.add_argument('--apply', action='store_true', help='Actually perform operations')
    parser.add_argument('--verbose', action='store_true', help='Verbose output')
    parser.add_argument('--skip-processed', action='store_true', help='Skip already processed files')
    parser.add_argument('--force-metadata', action='store_true', help='Rewrite metadata even if marker exists')
    parser.add_argument('--date-folder', action='store_true', help='Sort into YYYY/MM subfolders')
    parser.add_argument('--require-metadata', action='store_true', help='Abort rename if metadata write fails')
    parser.add_argument('--json-report', help='Save JSON report to file')

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
    )
    sorter.run()

    if args.json_report:
        sorter.save_json_report(Path(args.json_report))
        print(f"\nJSON report saved to {args.json_report}")

    sys.exit(0 if not sorter.errors else 1)


if __name__ == '__main__':
    main()
