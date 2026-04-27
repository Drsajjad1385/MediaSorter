#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MediaDeduper – Fast, accurate duplicate media finder.
Uses a 3‑stage pipeline: file size → partial hash (4 KiB) → full hash.
Only touches disk when necessary, parallelisable if needed.
"""

import argparse
import hashlib
import os
import sys
from pathlib import Path
from typing import Dict, List, Set, Tuple

try:
    import xxhash
    HAS_XXHASH = True
except ImportError:
    HAS_XXHASH = False

HASH_FIRST_CHUNK = 4096   # 4 KiB for stage 2
READ_CHUNK = 64 * 1024    # 64 KiB for full hash

class MediaDeduper:
    """Deduplicates media files based on content, not metadata."""

    IMAGE_EXT = {'.jpg', '.jpeg', '.png', '.webp', '.heic', '.heif', '.tiff', '.tif'}
    VIDEO_EXT = {'.mp4', '.mkv', '.mov', '.webm', '.avi', '.m4v', '.3gp', '.wmv', '.flv'}

    def __init__(self, target_dir: Path, recursive: bool = False, apply: bool = False, action: str = 'report',
                 dest_dir: Path = None):
        self.target_dir = target_dir.resolve()
        self.recursive = recursive
        self.apply = apply
        self.action = action          # 'report', 'move', 'delete'
        self.dest_dir = dest_dir      # for move action

        self.duplicates: Dict[str, List[Path]] = {}  # hash -> list of paths (grouped)
        self.errors: List[str] = []

    def _scan_files(self) -> List[Path]:
        pattern = '*'
        if self.recursive:
            glob_method = self.target_dir.rglob
        else:
            glob_method = self.target_dir.glob
        files = []
        for p in glob_method(pattern):
            if p.is_file() and p.suffix.lower() in self.IMAGE_EXT | self.VIDEO_EXT:
                # Skip hidden files / dotfiles
                if not p.name.startswith('.'):
                    files.append(p)
        return files

    def _hash_file_full(self, file_path: Path) -> str:
        """Full hash of entire file (xxh64 if available, else sha256)."""
        if HAS_XXHASH:
            hasher = xxhash.xxh64()
        else:
            hasher = hashlib.sha256()
        try:
            with open(file_path, 'rb') as f:
                while chunk := f.read(READ_CHUNK):
                    hasher.update(chunk)
        except Exception as e:
            self.errors.append(f"Read error {file_path}: {e}")
            return ""
        return hasher.hexdigest()

    def _hash_chunk(self, file_path: Path, size: int) -> str:
        """Hash the first `size` bytes of a file."""
        if HAS_XXHASH:
            hasher = xxhash.xxh64()
        else:
            hasher = hashlib.sha256()
        try:
            with open(file_path, 'rb') as f:
                data = f.read(size)
                hasher.update(data)
        except Exception as e:
            self.errors.append(f"Read error {file_path}: {e}")
            return ""
        return hasher.hexdigest()

    def run(self):
        print("Scanning files...")
        files = self._scan_files()
        print(f"Found {len(files)} media files.\n")

        # Stage 1: group by file size
        size_map: Dict[int, List[Path]] = {}
        for fp in files:
            fsize = fp.stat().st_size
            size_map.setdefault(fsize, []).append(fp)

        # Keep only sizes with multiple files
        candidates = {sz: flist for sz, flist in size_map.items() if len(flist) > 1}
        print(f"After size filter: {sum(len(v) for v in candidates.values())} files in {len(candidates)} groups.")
        if not candidates:
            print("No potential duplicates (all file sizes unique).")
            return

        # Stage 2: partial hash (first 4 KiB) within each size group
        partial_map: Dict[str, List[Path]] = {}
        for sz, flist in candidates.items():
            for fp in flist:
                phash = self._hash_chunk(fp, HASH_FIRST_CHUNK)
                if phash:
                    key = f"{sz}|{phash}"
                    partial_map.setdefault(key, []).append(fp)

        # Further filter groups with multiple files
        final_candidates = {k: v for k, v in partial_map.items() if len(v) > 1}
        print(f"After partial‑hash filter: {sum(len(v) for v in final_candidates.values())} files in {len(final_candidates)} groups.")
        if not final_candidates:
            print("No duplicates after partial hash check.")
            return

        # Stage 3: full hash for remaining candidates
        duplicate_groups: Dict[str, List[Path]] = {}
        for group_key, flist in final_candidates.items():
            for fp in flist:
                fhash = self._hash_file_full(fp)
                if fhash:
                    duplicate_groups.setdefault(fhash, []).append(fp)

        # Keep only groups with more than one file (exact matches)
        self.duplicates = {h: paths for h, paths in duplicate_groups.items() if len(paths) > 1}
        print(f"Exact duplicates found: {len(self.duplicates)} groups, "
              f"{sum(len(v) for v in self.duplicates.values())} duplicate files in total.\n")

        # Report / Act
        if not self.duplicates:
            print("No duplicates found.")
            return

        # Print groups
        group_num = 1
        for fhash, paths in self.duplicates.items():
            print(f"--- Duplicate group #{group_num} (hash: {fhash[:12]}...) ---")
            for p in paths:
                print(f"  {p}")
            group_num += 1

        if self.apply and self.action != 'report':
            self._apply_action()

    def _apply_action(self):
        """Perform move or delete on duplicates, keeping the first file in each group."""
        print(f"\nAction: {self.action.upper()} duplicates...")
        for paths in self.duplicates.values():
            # Keep the first file, process the rest
            keep = paths[0]
            for dup in paths[1:]:
                try:
                    if self.action == 'delete':
                        dup.unlink()
                        print(f"  Deleted: {dup}")
                    elif self.action == 'move':
                        if not self.dest_dir:
                            print("Error: --dest required for move action.", file=sys.stderr)
                            return
                        self.dest_dir.mkdir(parents=True, exist_ok=True)
                        dup.rename(self.dest_dir / dup.name)
                        print(f"  Moved: {dup} -> {self.dest_dir / dup.name}")
                except Exception as e:
                    self.errors.append(f"Action failed on {dup}: {e}")


def main():
    parser = argparse.ArgumentParser(description="MediaDeduper – find & handle duplicate media files")
    parser.add_argument('--target', required=True, help='Directory to scan')
    parser.add_argument('--recursive', action='store_true', help='Scan subdirectories')
    parser.add_argument('--apply', action='store_true', help='Actually perform actions (delete/move)')
    parser.add_argument('--action', choices=['report', 'delete', 'move'], default='report',
                        help='What to do with duplicates (default: report)')
    parser.add_argument('--dest', help='Destination folder for move action')
    args = parser.parse_args()

    target = Path(args.target)
    if not target.is_dir():
        print(f"Error: '{args.target}' is not a directory.", file=sys.stderr)
        sys.exit(1)
    dest = Path(args.dest) if args.dest else None

    deduper = MediaDeduper(
        target_dir=target,
        recursive=args.recursive,
        apply=args.apply,
        action=args.action,
        dest_dir=dest,
    )
    deduper.run()
    if deduper.errors:
        print("\nErrors encountered:")
        for e in deduper.errors[:10]:
            print(f"  {e}")
    sys.exit(0 if not deduper.errors else 1)


if __name__ == '__main__':
    main()
