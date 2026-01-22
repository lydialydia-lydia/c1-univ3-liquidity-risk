"""
Swap execution simulator (simplified).

Purpose:
Given a depth proxy curve or tick liquidity profile, approximate slippage
for trade sizes. This is NOT a perfect reproduction of Uniswap v3 swap math,
but it is sufficient for a C01 "risk analytics" notebook and produces
stable, interpretable slippage curves.

If you later want exact swap math, we can upgrade this module.
"""

from __future__ import annotations
import numpy as np
import pandas as pd


def slippage_from_depth_proxy(
    depth_curve: pd.DataFrame,
    trade_sizes: np.ndarray,
    depth_col: str = "depth_proxy",
    k: float = 1e-9,
) -> pd.DataFrame:
    """
    Map depth proxy -> slippage using a simple functional form.

    Intuition:
    - Larger depth means smaller slippage for a given trade.
    - use slippage ≈ k * trade_size / (depth_proxy + eps).

    Parameters
    ----------
    depth_curve : DataFrame with depth proxy at multiple price moves.
    trade_sizes : array of trade sizes in quote units.
    k          : scaling constant to keep numbers in a reasonable range.

    Returns
    -------
    DataFrame with columns:
    - trade_size
    - implied_slippage (for a representative depth level)
    """
    eps = 1e-12
    # Use local depth near small price moves as "available liquidity"
    depth_level = float(depth_curve.sort_values("pct_move").iloc[len(depth_curve) // 2][depth_col])
    out = []
    for q in trade_sizes:
        slip = k * float(q) / (depth_level + eps)
        out.append({"trade_size": float(q), "implied_slippage": float(slip)})
    return pd.DataFrame(out)
