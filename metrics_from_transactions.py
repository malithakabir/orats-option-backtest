"""metrics_from_transactions.py
================================

Given a *transactions.csv* (one row per option‑leg transaction produced by
*orats_backtest_v12.py*), this script rebuilds each **vertical‑spread trade**
(signal_id + name) and computes high‑level performance statistics.

Metrics produced per trade
-------------------------
* **debit**      – cash paid to open the spread (positive number).
* **pnl_net**    – net profit/loss *after fees*.
* **return**     – ``pnl_net / debit``.
* **hit_rate**   – 1 if ``pnl_net > 0`` else 0.
* **spot_return** – underlying directional %‑move, signed so that a *correct*
  move is positive (call: rise, put: fall).
* **spot_hit_rate** – 1 if ``spot_return > 0`` else 0.

Aggregated output
-----------------
After rebuilding trades the script prints a summary table identical to the
example you gave::

    name  count   return  spot_return  hit_rate  spot_hit_rate
    ...

as well as an overall line ("ALL") for portfolio‑wide averages.

Usage
-----
$ python metrics_from_transactions.py --tx transactions.csv --out metrics.csv
"""

from __future__ import annotations

import argparse
from pathlib import Path
import logging
import sys
from typing import List

import pandas as pd

LOGGER = logging.getLogger("txn_metrics")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def parse_cli(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser("Compute performance metrics from transactions.csv")
    p.add_argument("--tx", type=Path, required=True, help="Path to transactions.csv")
    p.add_argument("--out", type=Path, default=Path("metrics.csv"), help="CSV to write metrics summary")
    return p.parse_args(argv)

# -----------------------------------------------------------------------------
# Core logic
# -----------------------------------------------------------------------------

def rebuild_trades(tx: pd.DataFrame) -> pd.DataFrame:
    """From leg‑level *tx* build one row per vertical spread trade."""

    trades = []

    # Identify a trade by (signal_id, name)
    gcols = ["signal_id", "name"]
    for (signal_id, name), grp in tx.groupby(gcols):
        # split open/close legs
        open_legs  = grp[grp["order"].str.endswith("Open")]
        close_legs = grp[grp["order"].str.endswith("Close")]

        if open_legs.empty or close_legs.empty:
            continue  # malformed

        debit = -open_legs["credit"].sum()  # debit > 0
        fees  = grp["transaction_fee"].sum()
        pnl_net = grp["credit"].sum() - fees
        ret = pnl_net / debit if debit else 0.0
        hit = 1.0 if pnl_net > 0 else 0.0

        # underlying movement
        spot_entry = open_legs.iloc[0]["underlier"]
        spot_exit  = close_legs.iloc[0]["underlier"]
        right      = open_legs.iloc[0]["right"].lower()  # 'call' or 'put'

        raw_ret = (spot_exit - spot_entry) / spot_entry
        spot_ret = raw_ret if right == "call" else -raw_ret
        spot_hit = 1.0 if spot_ret > 0 else 0.0

        trades.append(
            {
                "signal_id": signal_id,
                "name": name,
                "debit": round(debit, 2),
                "pnl_net": round(pnl_net, 2),
                "return": round(ret, 4),
                "hit_rate": hit,
                "spot_return": round(spot_ret, 4),
                "spot_hit_rate": spot_hit,
            }
        )

    return pd.DataFrame(trades)


def summarise(trades: pd.DataFrame) -> pd.DataFrame:
    """Aggregate per‑strategy metrics (same shape as user example)."""

    if trades.empty:
        LOGGER.warning("No trades to summarise.")
        return trades

    summary = trades.groupby("name").agg(
        count=("name", "count"),
        return_mean=("return", "mean"),
        spot_return_mean=("spot_return", "mean"),
        hit_rate=("hit_rate", "mean"),
        spot_hit_rate=("spot_hit_rate", "mean"),
    ).reset_index()

    # rename columns exactly as requested
    summary.rename(
        columns={
            "return_mean": "return",
            "spot_return_mean": "spot_return",
        },
        inplace=True,
    )

    # # overall line
    # overall = pd.DataFrame(
    #     {
    #         "name": ["ALL"],
    #         "count": [summary["count"].sum()],
    #         "return": [summary["return"].mean()],
    #         "spot_return": [summary["spot_return"].mean()],
    #         "hit_rate": [summary["hit_rate"].mean()],
    #         "spot_hit_rate": [summary["spot_hit_rate"].mean()],
    #     }
    # )
    # summary = pd.concat([summary, overall], ignore_index=True)
    return summary

# -----------------------------------------------------------------------------
# Main entry
# -----------------------------------------------------------------------------

def main(argv: List[str] | None = None):
    args = parse_cli(argv or sys.argv[1:])

    tx_df = pd.read_csv(args.tx)
    LOGGER.info("Transactions loaded: %d rows", len(tx_df))

    trades_df = rebuild_trades(tx_df)
    LOGGER.info("Trades rebuilt: %d", len(trades_df))

    summary_df = summarise(trades_df)
    summary_df.to_csv(args.out, index=False)
    LOGGER.info("Metrics written to %s", args.out)

    # print to console too
    pd.set_option("display.max_columns", None)
    print("\n===== Performance summary =====")
    print(summary_df.to_string(index=False))


if __name__ == "__main__":
    main()
