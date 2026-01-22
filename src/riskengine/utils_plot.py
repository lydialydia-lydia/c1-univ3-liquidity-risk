"""
Plot helpers: consistent titles/labels and saving figures.
"""

from __future__ import annotations
from pathlib import Path
import matplotlib.pyplot as plt


def save_figure(fig: plt.Figure, outpath: Path, dpi: int = 200) -> None:
    """
    Save a matplotlib figure to disk.
    """
    outpath.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(outpath, dpi=dpi, bbox_inches="tight")
