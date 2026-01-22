"""
Liquidity depth curve construction.

Goal:
Given a tick-level liquidity map, estimate "how much volume can be absorbed"
for a given price move up/down.

This is a simplified depth model for C01:
- treat liquidity_net as the incremental change in active liquidity when crossing a tick.
- build an "active liquidity" profile by cumulatively summing liquidity_net around current_tick.
"""

from __future__ import annotations
import numpy as np
import pandas as pd
from .univ3_math import tick_to_price


def build_active_liquidity_profile(
    ticks: pd.DataFrame, current_tick: int, tick_window: int = 50_000
) -> pd.DataFrame:
    """
    Build an approximate active liquidity profile around current_tick.

    Returns dataframe with:
    - tick
    - price
    - active_liquidity (approx)
    """
    df = ticks[["tick", "liquidity_net"]].copy()
    df = df.sort_values("tick").reset_index(drop=True)

    # Focus on a window around the current tick for efficiency
    df = df[(df["tick"] >= current_tick - tick_window) & (df["tick"] <= current_tick + tick_window)].copy()
    df["price"] = df["tick"].apply(tick_to_price)

    # Approximate active liquidity:
    # Anchor at current_tick with an arbitrary baseline and accumulate liquidity_net.
    # In a production implementation you would compute exact L from positions.
    df["active_liquidity"] = df["liquidity_net"].cumsum()

    # Re-anchor so that liquidity near current tick is centered and non-negative for plotting
    idx0 = (df["tick"] - current_tick).abs().idxmin()
    anchor = float(df.loc[idx0, "active_liquidity"])
    df["active_liquidity"] = df["active_liquidity"] - anchor
    df["active_liquidity"] = df["active_liquidity"].abs()

    return df


def depth_curve_from_profile(
    profile: pd.DataFrame,
    current_price: float,
    pct_moves: np.ndarray,
) -> pd.DataFrame:
    """
    Compute a simple depth curve:
    For each pct_move, sum liquidity in the price interval between current and moved price.

    Returns columns:
    - pct_move
    - price_target
    - depth_proxy (unitless proxy)
    """
    out = []
    for pm in pct_moves:
        price_target = current_price * (1.0 + pm)
        p_min, p_max = sorted([current_price, price_target])
        mask = (profile["price"] >= p_min) & (profile["price"] <= p_max)
        depth_proxy = float(profile.loc[mask, "active_liquidity"].sum())
        out.append({"pct_move": float(pm), "price_target": float(price_target), "depth_proxy": depth_proxy})
    return pd.DataFrame(out)
