"""
Snapshot format for Uniswap v3 pool state.

A snapshot is a fixed-time view used for reproducible analytics.
"""

from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import pandas as pd


@dataclass
class UniV3Snapshot:
    """
    Snapshot of a Uniswap v3 pool at a specific block/time.

    Required columns for ticks dataframe:
    - tick: int
    - liquidity_net: float (or int)
    - liquidity_gross: float (optional but useful)
    """
    pool_name: str
    fee_tier: str
    timestamp_utc: str  # ISO string
    current_tick: int
    current_price: float
    ticks: pd.DataFrame  # must include tick + liquidity_net

    def validate(self) -> None:
        required = {"tick", "liquidity_net"}
        missing = required - set(self.ticks.columns)
        if missing:
            raise ValueError(f"Ticks dataframe missing columns: {missing}")
        if self.ticks["tick"].isna().any():
            raise ValueError("Ticks dataframe contains NaN ticks.")
        if self.ticks["liquidity_net"].isna().any():
            raise ValueError("Ticks dataframe contains NaN liquidity_net.")


def save_snapshot(snapshot: UniV3Snapshot, outpath: Path) -> None:
    """
    Save snapshot to a parquet file:
    - metadata in a small sidecar JSON (simple & robust)
    - ticks as parquet
    """
    outpath.parent.mkdir(parents=True, exist_ok=True)
    tick_path = outpath.with_suffix(".ticks.parquet")
    meta_path = outpath.with_suffix(".meta.json")

    snapshot.validate()
    snapshot.ticks.to_parquet(tick_path, index=False)

    meta = {
        "pool_name": snapshot.pool_name,
        "fee_tier": snapshot.fee_tier,
        "timestamp_utc": snapshot.timestamp_utc,
        "current_tick": snapshot.current_tick,
        "current_price": snapshot.current_price,
        "ticks_file": tick_path.name,
    }
    pd.Series(meta).to_json(meta_path, indent=2)


def load_snapshot(basepath: Path) -> UniV3Snapshot:
    """
    Load snapshot from:
    - basepath.meta.json
    - basepath.ticks.parquet
    """
    meta_path = basepath.with_suffix(".meta.json")
    tick_path = basepath.with_suffix(".ticks.parquet")

    if not meta_path.exists():
        raise FileNotFoundError(f"Missing meta file: {meta_path}")
    if not tick_path.exists():
        raise FileNotFoundError(f"Missing ticks file: {tick_path}")

    meta = pd.read_json(meta_path, typ="series").to_dict()
    ticks = pd.read_parquet(tick_path)

    snap = UniV3Snapshot(
        pool_name=str(meta["pool_name"]),
        fee_tier=str(meta["fee_tier"]),
        timestamp_utc=str(meta["timestamp_utc"]),
        current_tick=int(meta["current_tick"]),
        current_price=float(meta["current_price"]),
        ticks=ticks,
    )
    snap.validate()
    return snap
