# src/riskengine/scripts/fetch_snapshot_ethusdc_mainnet.py
# English comments only

import os
import json
import time
from pathlib import Path
from typing import Dict, Any, List, Optional

import pandas as pd
import requests


SUBGRAPH_ID = "4cKy6QQMc5tpfdx8yxfYeb9TLZmgLQe44ddW1G7NwkA6"  # Uniswap V3 Ethereum (your current)
POOL_ADDRESS = "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"   # WETH/USDC 0.05% (commonly used)


def _endpoint() -> str:
    api_key = os.environ.get("GRAPH_API_KEY")
    if not api_key:
        raise RuntimeError("Missing env var GRAPH_API_KEY (do NOT hardcode keys in notebooks).")
    return f"https://gateway.thegraph.com/api/{api_key}/subgraphs/id/{SUBGRAPH_ID}"


def gql_post(query: str, variables: Optional[Dict[str, Any]] = None, retries: int = 3) -> Dict[str, Any]:
    url = _endpoint()
    last_err = None
    for i in range(retries):
        r = requests.post(url, json={"query": query, "variables": variables or {}}, timeout=60)
        try:
            data = r.json()
        except Exception:
            last_err = {"status": r.status_code, "text": r.text[:500]}
            time.sleep(1.0 * (i + 1))
            continue
        if "errors" in data:
            last_err = data["errors"]
            time.sleep(1.0 * (i + 1))
            continue
        return data["data"]
    raise RuntimeError(f"GraphQL request failed after {retries} retries: {last_err}")


def query_fields() -> List[str]:
    q = """
    query {
      __schema { queryType { fields { name } } }
    }
    """
    data = gql_post(q)
    return [x["name"] for x in data["__schema"]["queryType"]["fields"]]


def fetch_pool_meta(pool_id: str) -> Dict[str, Any]:
    fields = set(query_fields())

    # Variant A: pool(id: ...)
    if "pool" in fields:
        q = """
        query Pool($id: ID!) {
          pool(id: $id) {
            id feeTier tick sqrtPrice
            liquidity
            token0 { id symbol decimals }
            token1 { id symbol decimals }
          }
        }
        """
        data = gql_post(q, {"id": pool_id})
        if data.get("pool") is None:
            raise RuntimeError("Pool not found via `pool(id:)` — check POOL_ADDRESS / endpoint.")
        return data["pool"]

    # Variant B: pools(where: {id: ...})
    if "pools" in fields:
        q = """
        query Pools($id: String!) {
          pools(where: { id: $id }) {
            id feeTier tick sqrtPrice
            liquidity
            token0 { id symbol decimals }
            token1 { id symbol decimals }
          }
        }
        """
        data = gql_post(q, {"id": pool_id})
        arr = data.get("pools") or []
        if not arr:
            raise RuntimeError("Pool not found via `pools(where:{id})` — check POOL_ADDRESS / endpoint.")
        return arr[0]

    raise RuntimeError("Neither `pool` nor `pools` exists on Query. You likely used the wrong subgraph schema.")


def fetch_ticks(pool_id: str, page_size: int = 1000) -> pd.DataFrame:
    fields = set(query_fields())

    # Most common: ticks(where: {pool: "<poolId>"})
    if "ticks" not in fields:
        raise RuntimeError("Query has no `ticks` field. This subgraph schema is not UniswapV3-like.")

    q = """
    query Ticks($pool: String!, $first: Int!, $skip: Int!) {
      ticks(
        first: $first,
        skip: $skip,
        where: { pool: $pool },
        orderBy: tickIdx,
        orderDirection: asc
      ) {
        id
        tickIdx
        liquidityGross
        liquidityNet
        price0
        price1
      }
    }
    """

    rows: List[Dict[str, Any]] = []
    skip = 0
    while True:
        data = gql_post(q, {"pool": pool_id, "first": page_size, "skip": skip})
        batch = data.get("ticks") or []
        if not batch:
            break
        rows.extend(batch)
        skip += page_size

    if not rows:
        raise RuntimeError("Fetched 0 ticks. The `where` filter may differ on this endpoint; run schema introspection and adjust.")

    df = pd.DataFrame(rows)
    # Normalize numeric columns
    for c in ["tickIdx", "liquidityGross", "liquidityNet"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.sort_values("tickIdx").reset_index(drop=True)


def main(out_dir: str = "data/processed", prefix: str = "ETHUSDC_0p05_mainnet") -> None:
    outp = Path(out_dir)
    outp.mkdir(parents=True, exist_ok=True)

    pool = fetch_pool_meta(POOL_ADDRESS)
    ticks = fetch_ticks(POOL_ADDRESS)

    meta_path = outp / f"{prefix}.meta.json"
    ticks_path = outp / f"{prefix}.ticks.parquet"

    meta_path.write_text(json.dumps(pool, indent=2))
    ticks.to_parquet(ticks_path, index=False)

    print("Saved:", meta_path)
    print("Saved:", ticks_path)
    print("rows:", len(ticks))


if __name__ == "__main__":
    main()
