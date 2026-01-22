"""
Uniswap v3 math utilities.

Note:
- tick <-> price mapping assumes token1/token0 price convention.
- For most C01 analytics, you only need tick->price and basic conversions.
"""

import math


def tick_to_price(tick: int) -> float:
    """
    Convert tick to price using Uniswap v3 convention:
    price = 1.0001^tick

    This returns token1 per token0 (commonly quote per base).
    """
    return 1.0001 ** tick


def price_to_tick(price: float) -> int:
    """
    Convert price to nearest tick.
    """
    if price <= 0:
        raise ValueError("Price must be positive.")
    return int(round(math.log(price, 1.0001)))


def pct_move_to_price(price0: float, pct_move: float) -> float:
    """
    Apply a percentage move to price.
    pct_move = +0.01 means +1%.
    """
    return price0 * (1.0 + pct_move)
