"""
00 - Fetch a real Uniswap v3 tick snapshot from The Graph (Ethereum mainnet).

Output files (basename):
- <OUT_DIR>/<BASENAME>.meta.json
- <OUT_DIR>/<BASENAME>.ticks.parquet

This script is intended to be run ONCE to produce a fixed snapshot.
After that, notebooks (01/02/03) should only download & read the snapshot
from GitHub (no more subgraph queries during analysis).
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from datetime import datetime, timezone

import requests
import pandas as pd

from riskengine.univ3_snapshot import UniV3Snapshot, save_snapshot



# Fixed targets (Ethereum mainnet)
POOL_ADDRESS = "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"  # USDC/WETH 0.05%
SUBGRAPH_ID = "4cKy6QQMc5tpfdx8yxfYeb9TLZmgLQe44ddW1G7NwkA6"  # Uniswap V3 Ethereum
PAGE_SIZE = 1000
SLEEP_BETWEEN_CALLS = 0.2

# Tick window around current tick (adjustable)
TICK_WINDOW = 50_000


def gql_post(url: str, query: str, variables: dict | None = None, timeout: int = 60) -> dict:
    """POST a GraphQL request and return data."""
    payload = {"query": query, "variables": variables or {}}
    r = requests.post(url, json=payload, timeout=timeout)
    r.raise_for_status()
    j = r.json()
    if "errors" in j:
        raise RuntimeError(j["errors"])
    return j["data"]


def price_from_sqrtPriceX96(sqrtPriceX96: int, decimals0: int, decimals1: int) -> float:
    """
    Uniswap v3 convention:
    sqrtPriceX96 = sqrt(token1/token0) * 2^96

    Human-readable token1/token0 price:
    (sqrtPriceX96 / 2^96)^2 * 10^(decimals0 - decimals1)
    """
    ratio = (sqrtPriceX96 / (2**96)) ** 2
    return ratio * (10 ** (decimals0 - decimals1))


def main() -> None:
    # The Graph Gateway endpoint (requires API key)
    graph_api_key = os.getenv("GRAPH_API_KEY", "").strip()
    if not graph_api_key:
        raise RuntimeError("GRAPH_API_KEY is not set. Please set it before running this script.")

    subgraph_url = f"https://gateway.thegraph.com/api/{graph_api_key}/subgraphs/id/{SUBGRAPH_ID}"
    print("Using subgraph URL:", subgraph_url)

    out_dir = Path(os.getenv("OUT_DIR", "/content/c01_snapshots"))
    out_dir.mkdir(parents=True, exist_ok=True)

    basename = os.getenv("BASENAME", "ETHUSDC_0p05_mainnet")
    out_base = out_dir / basename

    # -----------------------------
    # 1) Fetch pool state
    # -----------------------------
    POOL_QUERY = """
    query PoolState($id: ID!) {
      pool(id: $id) {
        id
        feeTier
        tick
        sqrtPrice
        sqrtPriceX96
        token0 { symbol decimals }
        token1 { symbol decimals }
      }
    }
    """
    data = gql_post(subgraph_url, POOL_QUERY, {"id": POOL_ADDRESS})
    pool = data.get("pool")
    if pool is None:
        raise RuntimeError("Pool not found on the subgraph. Check POOL_ADDRESS / SUBGRAPH_ID.")

    fee_tier = str(pool["feeTier"])
    current_tick = int(pool["tick"])

    token0_symbol = pool["token0"]["symbol"]
    token1_symbol = pool["token1"]["symbol"]
    dec0 = int(pool["token0"]["decimals"])
    dec1 = int(pool["token1"]["decimals"])

    sqrtp = pool.get("sqrtPriceX96") or pool.get("sqrtPrice")
    if sqrtp is None:
        raise RuntimeError("Neither sqrtPriceX96 nor sqrtPrice is present in pool response.")
    sqrtPriceX96 = int(sqrtp)

    current_price = float(price_from_sqrtPriceX96(sqrtPriceX96, dec0, dec1))

    print("Pool:", pool["id"])
    print("Pair:", f"{token0_symbol}/{token1_symbol}", "fee:", fee_tier)
    print("Current tick:", current_tick)
    print("Price token1/token0:", current_price)

    # -----------------------------
    # 2) Fetch ticks (liquidityNet/Gross) within window
    # -----------------------------
    min_tick = current_tick - TICK_WINDOW
    max_tick = current_tick + TICK_WINDOW

    TICKS_QUERY = """
    query TicksPage($pool: String!, $minTick: Int!, $maxTick: Int!, $last: Int!, $first: Int!) {
      ticks(
        first: $first
        orderBy: tickIdx
        orderDirection: asc
        where: {
          poolAddress: $pool
          tickIdx_gte: $minTick
          tickIdx_lte: $maxTick
          tickIdx_gt: $last
        }
      ) {
        tickIdx
        liquidityGross
        liquidityNet
      }
    }
    """

    rows = []
    last_tick = min_tick - 1

    while True:
        page = gql_post(
            subgraph_url,
            TICKS_QUERY,
            {
                "pool": POOL_ADDRESS,
                "minTick": int(min_tick),
                "maxTick": int(max_tick),
                "last": int(last_tick),
                "first": int(PAGE_SIZE),
            },
        )["ticks"]

        if not page:
            break

        for t in page:
            rows.append(
                {
                    "tick": int(t["tickIdx"]),
                    "liquidity_gross": float(t["liquidityGross"]),
                    "liquidity_net": float(t["liquidityNet"]),
                }
            )

        last_tick = int(page[-1]["tickIdx"])
        if len(page) < PAGE_SIZE:
            break

        time.sleep(SLEEP_BETWEEN_CALLS)

    ticks_df = pd.DataFrame(rows).sort_values("tick").reset_index(drop=True)
    print("Fetched ticks:", len(ticks_df))

    # -----------------------------
    # 3) Save snapshot using riskengine schema
    # -----------------------------
    snap = UniV3Snapshot(
        pool_name=f"{token0_symbol}/{token1_symbol}",
        fee_tier=fee_tier,
        timestamp_utc=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        current_tick=current_tick,
        current_price=current_price,
        ticks=ticks_df,
    )

    save_snapshot(snap, out_base)

    print("Saved snapshot:")
    print(" -", out_base.with_suffix(".meta.json"))
    print(" -", out_base.with_suffix(".ticks.parquet"))


if __name__ == "__main__":
    main()
