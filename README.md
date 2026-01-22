# c1-univ3-liquidity-risk

# C01 — Uniswap v3 Concentrated Liquidity Tail Risk & Slippage Curves

This project quantifies liquidity and execution risk in Uniswap v3:
- Liquidity distribution (tick / price ranges)
- Liquidity depth curves (price move → depth)
- Slippage / market impact curves (trade size → slippage)
- Tail risk (p95/p99 slippage, out-of-range probability under volatility regimes)

## Repo Layout
- `src/riskengine/` — reusable functions (imported by notebooks)
- `notebooks/` — analysis notebooks (thin; mostly plotting and interpretation)
- `data/processed/` — fixed pool snapshots for reproducible results
- `figures/` — saved charts for README / reporting

## Reproducible Setup (Colab / local)
```bash
git clone <YOUR_REPO_URL>
cd <REPO>
pip install -r requirements.txt
pip install -e .
