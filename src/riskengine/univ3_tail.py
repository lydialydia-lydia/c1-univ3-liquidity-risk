"""
Tail risk layer:
- Out-of-range probability for LP ranges
- p95/p99 tail slippage under different volatility regimes

Model price as a simple lognormal diffusion for demonstration.
can later replace with realized volatility + jumps.
"""

from __future__ import annotations
import numpy as np
import pandas as pd


def simulate_price_paths_gbm(
    price0: float,
    sigma_annual: float,
    horizon_days: int,
    steps_per_day: int,
    n_paths: int,
    seed: int = 42,
) -> np.ndarray:
    """
    Simulate GBM price paths.

    Returns array shape: (n_paths, n_steps+1)
    """
    rng = np.random.default_rng(seed)
    dt = 1.0 / (365.0 * steps_per_day)
    n_steps = horizon_days * steps_per_day

    # Drift set to 0 for risk metrics (can be added if needed)
    increments = (sigma_annual * np.sqrt(dt)) * rng.standard_normal((n_paths, n_steps))
    log_paths = np.cumsum(increments, axis=1)
    log_paths = np.concatenate([np.zeros((n_paths, 1)), log_paths], axis=1)
    paths = price0 * np.exp(log_paths)
    return paths


def out_of_range_probability(
    price_paths: np.ndarray,
    price0: float,
    lp_range_pct: float,
) -> float:
    """
    Probability that price leaves [price0*(1-r), price0*(1+r)] at ANY time.
    """
    low = price0 * (1.0 - lp_range_pct)
    high = price0 * (1.0 + lp_range_pct)
    breached = (price_paths < low) | (price_paths > high)
    return float(breached.any(axis=1).mean())


def tail_slippage_table(
    slippage_model_func,
    vol_regimes_annual: list[float],
    trade_sizes: np.ndarray,
    n_mc: int = 2000,
    seed: int = 42,
) -> pd.DataFrame:
    """
    Produce a p95/p99 tail slippage table over volatility regimes.

    slippage_model_func(sigma_annual, trade_size, rng) -> slippage (float)
    """
    rng = np.random.default_rng(seed)
    rows = []
    for sigma in vol_regimes_annual:
        for q in trade_sizes:
            samples = [float(slippage_model_func(sigma, float(q), rng)) for _ in range(n_mc)]
            p95 = float(np.quantile(samples, 0.95))
            p99 = float(np.quantile(samples, 0.99))
            rows.append({"sigma_annual": float(sigma), "trade_size": float(q), "p95_slippage": p95, "p99_slippage": p99})
    return pd.DataFrame(rows)
