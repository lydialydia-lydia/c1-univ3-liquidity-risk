"""
I/O helpers: consistent paths, saving outputs, and lightweight caching.
"""

from __future__ import annotations
from pathlib import Path


def repo_root() -> Path:
    """
    Return repository root assuming this file is at src/riskengine/utils_io.py.
    """
    return Path(__file__).resolve().parents[2]


def data_dir() -> Path:
    return repo_root() / "data"


def processed_dir() -> Path:
    return data_dir() / "processed"


def figures_dir() -> Path:
    return repo_root() / "figures"


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path
