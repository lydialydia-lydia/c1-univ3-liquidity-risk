"""
Central configuration and defaults.

Keep ALL defaults here so notebooks stay minimal and reproducible.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class DefaultC01Config:
    # Reproducibility
    seed: int = 42

    # Default trade sizes (quote currency units, e.g., USDC)
    trade_sizes_quote: tuple[float, ...] = (1_000, 10_000, 100_000, 1_000_000)

    # LP ranges (percentage around spot price)
    lp_ranges_pct: tuple[float, ...] = (0.01, 0.02, 0.05)

    # Example annualized vol regimes (you can later estimate from historical data)
    vol_regimes_annual: tuple[float, ...] = (0.30, 0.60, 1.00)

    # Simulation horizon for out-of-range risk
    horizon_days: int = 7
    steps_per_day: int = 1440  # minute steps
