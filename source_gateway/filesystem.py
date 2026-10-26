"""Durable publication helpers for fenced, private migration directories."""

import os
from pathlib import Path
import stat


def sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def durable_tree(root: Path) -> None:
    """Flush file bytes, then directory entries from leaves to root."""
    root = Path(root)
    if root.is_symlink() or not root.is_dir():
        raise ValueError("invalid migration directory")
    for directory, folders, files in os.walk(root, topdown=False, followlinks=False):
        for name in folders:
            path = Path(directory) / name
            if path.is_symlink() or not path.is_dir():
                raise ValueError("unsafe migration directory")
        for name in files:
            path = Path(directory) / name
            if not stat.S_ISREG(path.lstat().st_mode):
                raise ValueError("unsafe migration file")
            descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        sync_directory(Path(directory))
