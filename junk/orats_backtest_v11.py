r"""
orats_backtest_v11.py
=====================

A **command‑line tool** for back‑testing simple vertical option spreads (debit call‑spreads for
bullish signals, debit put‑spreads for bearish signals) driven by signal rows and ORATS
snapshot data.

--------------------------------------------------------------------
Quick start
--------------------------------------------------------------------
$ python orats_backtest_v11.py \
    --signals part_001.csv \
    --orats part_001_entry.csv.gz \
    --exit exit.csv.gz \
    --deltas 0.25 0.30 0.35 \
    --max_spread_pct 0.15 \
    --min_rr 0.8 \
    --max_dte 8 \
    --out trades.csv

--------------------------------------------------------------------
Workflow
--------------------------------------------------------------------
1. **Read inputs**
   * *Signal* file provides time‑stamped bullish/bearish events (column
     ``trend``) in UTC.
   * *ORATS entry* file provides quote snapshots for every listed option.
   * *Exit* file provides the underlying *spotPrice/stockPrice* on every
     trading day, used to mark the trade on option expiry.

2. **Match snapshots**
   For each signal row we take the *closest* ORATS snapshot **before or at or after** the
   signal timestamp for the same ticker.

3. **Filter snapshots**
   After that the snapshot is filtered to only include options with a DTE
   between 1 and *max_dte* (default 8).

4. **Construct candidate verticals**
   For each target absolute ``delta`` (e.g. 0.25):
   * *Long* leg: option with absolute delta closest to target.
   * *Short* leg: next farther‑out‑of‑the‑money strike (call: higher strike,
     put: lower strike) so that the long/short leg deltas have the same sign.
   * The spread must obey::

         bid‑ask spread ≤ max_spread_pct × bid_price
         risk/reward      ≥ min_rr

5. **Price entry** (debit):
   * Pay *ask* for long leg, receive *bid* for short leg.

6. **Price exit** on *expirDate*:
   * Underlying spot on expiry (from *exit* file).
   * Vertical payoff calculated analytically.


--------------------------------------------------------------------
Assumptions & Notes
--------------------------------------------------------------------
* The script handles *debit* verticals only. Modify ``build_vertical`` if you
  need credit spreads.
* ``delta`` in ORATS is *signed*; calls are positive, puts negative.
* Signals with no matching ORATS snapshot (same ticker within the same trading
  day) are skipped with a warning.
* Uses pandas for data wrangling; suitable for millions of rows but assumes
  available memory.
"""


from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Iterable, List, Tuple

import pandas as pd

LOGGER = logging.getLogger("option_backtest")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# --------------------------------------------------------------------------------------
# Helper functions
# --------------------------------------------------------------------------------------

def parse_datetime(df: pd.DataFrame, col: str) -> None:
    """Convert *col* in *df* to pandas ``datetime64[ns, UTC]`` *in‑place*."""

    df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")


def nearest_snapshot(orats: pd.DataFrame, ticker: str, ts: pd.Timestamp) -> pd.DataFrame:
    """Return the slice of *orats* with *ticker* and the **earliest** ``quoteDate`` **on or
    after** *ts*.

    Parameters
    ----------
    orats
        ORATS DataFrame.
    ticker
        Ticker symbol.
    ts
        Signal timestamp (UTC).
    Returns
    -------
    pd.DataFrame
        A DataFrame slice (can be empty) of all options for that one snapshot.
    """

    df = orats[(orats["ticker"] == ticker) & (orats["quoteDate"] <= ts)]
    if not df.empty:
        first_ts = df["quoteDate"].max()
    else:
        df = orats[(orats["ticker"] == ticker) & (orats["quoteDate"] >= ts)]
        first_ts = df["quoteDate"].min()
    if df.empty:
        return df
    
    return df[df["quoteDate"] == first_ts]

def pick_leg(df: pd.DataFrame, target_delta: float, option_type: str) -> pd.Series | None:
    """Select the row whose |Δ| is closest to *target_delta* (sign matched)."""

    df_side = df.copy()
    if df_side.empty:
        return None

    df_side["abs_delta"] = df_side["delta"].abs()
    df_side["delta_diff"] = (df_side["abs_delta"] - target_delta).abs()
    return df_side.sort_values("delta_diff").iloc[0]


def build_vertical(
    snapshot: pd.DataFrame,
    option_type: str,
    target_delta: float,
    max_spread_pct: float,
    min_rr: float,
) -> Tuple[pd.Series, pd.Series] | None:
    """Return ``(long_leg, short_leg)`` or *None* if constraints fail."""

    long_leg = pick_leg(snapshot, target_delta, option_type)
    if long_leg is None:
        return None

    # ----- choose short leg -----
    if option_type == "call":
        cands = snapshot[(snapshot["strike"] > long_leg["strike"])]
    else:
        cands = snapshot[(snapshot["strike"] < long_leg["strike"])]

    if cands.empty:
        return None

    target = abs(long_leg["delta"]) / 2
    cands = cands.assign(delta_diff=(cands["delta"].abs() - target).abs())
    short_leg = cands.sort_values(["delta_diff", "strike"]).iloc[0]

    # ----- quality filters -----
    ask_long = long_leg[f"{option_type}AskPrice"]
    bid_long = long_leg[f"{option_type}BidPrice"]
    bid_short = short_leg[f"{option_type}BidPrice"]

    if pd.isna(ask_long) or pd.isna(bid_long) or pd.isna(bid_short) or bid_long == 0:
        return None

    if (ask_long - bid_long) / bid_long > max_spread_pct:
        return None

    width = abs(short_leg["strike"] - long_leg["strike"])
    debit = ask_long - bid_short
    if debit <= 0:
        return None

    rr = (width - debit) / debit
    if rr < min_rr:
        return None

    return long_leg, short_leg


def payoff_vertical(option_type: str, long_k: float, short_k: float, spot: float) -> float:
    """Intrinsic value at expiry of the *debit* vertical."""
    if option_type == "call":
        return max(0.0, spot - long_k) - max(0.0, spot - short_k)
    return max(0.0, long_k - spot) - max(0.0, short_k - spot)


# --------------------------------------------------------------------------------------
# Back‑tester
# --------------------------------------------------------------------------------------

class Backtester:
    def __init__(
        self,
        signals_path: Path,
        orats_path: Path,
        exit_path: Path,
        deltas: Iterable[float],
        max_spread_pct: float,
        min_rr: float,
        max_dte: int,
    ) -> None:
        self.signals_path = signals_path
        self.orats_path = orats_path
        self.exit_path = exit_path
        self.deltas = list(deltas)
        self.max_spread_pct = max_spread_pct
        self.min_rr = min_rr
        self.max_dte = max_dte

        LOGGER.info("Signals: %s", signals_path)
        LOGGER.info("Loading CSVs …")
        self.signals = pd.read_csv(signals_path)
        self.orats = pd.read_csv(orats_path, compression="infer")
        self.exit = pd.read_csv(exit_path, compression="infer")

        # ---- datetime ----
        parse_datetime(self.signals, "t")
        parse_datetime(self.orats, "quoteDate")
        parse_datetime(self.orats, "expirDate")
        parse_datetime(self.exit, "snapShotDate")

        self.exit.rename(columns={"snapShotDate": "exit_ts"}, inplace=True)
        self.exit["exit_date"] = self.exit["exit_ts"].dt.normalize()

    # ------------------------------------------------------------------
    def run(self) -> pd.DataFrame:
        """Execute back‑test and return trade log."""

        trades: List[dict] = []

        for i, sig in self.signals.iterrows():
            signal_id = sig["signal_id"]
            ticker = sig["ticker"]
            ts = sig["t"]
            otype = "call" if sig["trend"].lower() == "bullish" else "put"

            snapshot = nearest_snapshot(self.orats, ticker, ts)
            if snapshot.empty:
                continue

            # DTE filter first
            snap_dte = snapshot[(snapshot["dte"] <= self.max_dte) & (snapshot["dte"] > 1)]
            if snap_dte.empty:
                continue

            # Spot price at entry – same for all rows in this snapshot
            spot_entry = snap_dte.iloc[0]["spotPrice"] if not pd.isna(snap_dte.iloc[0]["spotPrice"]) else snap_dte.iloc[0]["stockPrice"]

            for d in self.deltas:
                legs = build_vertical(snap_dte, otype, d, self.max_spread_pct, self.min_rr)
                if legs is None:
                    continue

                long_leg, short_leg = legs

                width = abs(short_leg["strike"] - long_leg["strike"])
                debit = long_leg[f"{otype}AskPrice"] - short_leg[f"{otype}BidPrice"]
                rr = (width - debit) / debit

                expir = pd.to_datetime(long_leg["expirDate"], utc=True)
                exit_row = self.exit[(self.exit["ticker"] == ticker) & (self.exit["exit_date"] == expir.normalize())]
                if exit_row.empty:
                    continue

                spot_exit = exit_row.iloc[0]["spotPrice"] if not pd.isna(exit_row.iloc[0]["spotPrice"]) else exit_row.iloc[0]["stockPrice"]

                payoff = payoff_vertical(otype, long_leg["strike"], short_leg["strike"], spot_exit)
                pnl = payoff - debit
                ret = pnl / debit if debit else 0.0

                trades.append(
                    {
                        "signal_id": signal_id,
                        "signal_ts": ts,
                        "ticker": ticker,
                        "direction": otype,
                        "delta_target": d,
                        "quote_ts": long_leg["quoteDate"],
                        "expir": expir.normalize(),
                        "spot_entry": spot_entry,
                        "long_strike": long_leg["strike"],
                        "short_strike": short_leg["strike"],
                        "width": width,
                        "debit": debit,
                        "risk_reward": rr,
                        "spot_exit": spot_exit,
                        "payoff": payoff,
                        "pnl": pnl,
                        "return": ret,
                    }
                )

        trades_df = pd.DataFrame(trades)
        if trades_df.empty:
            LOGGER.warning("No trades produced.")
            return trades_df

        # ---- rounding & formatting ----
        money_cols = ["width", "debit", "payoff", "pnl", "spot_entry", "spot_exit", "long_strike", "short_strike"]
        ratio_cols = ["risk_reward", "return"]
        trades_df[money_cols] = trades_df[money_cols].round(2)
        trades_df[ratio_cols] = trades_df[ratio_cols].round(3)

        LOGGER.info("Trades processed: %d", len(trades_df))

        return trades_df


# --------------------------------------------------------------------------------------
# CLI – unchanged except help text
# --------------------------------------------------------------------------------------

def _parse_cli(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Back-test debit vertical spreads from signals + ORATS")
    p.add_argument("--signals", type=Path, required=True)
    p.add_argument("--orats", type=Path, required=True)
    p.add_argument("--exit", type=Path, required=True)
    p.add_argument("--deltas", type=float, nargs="*", default=[round(i / 100, 2) for i in range(10, 101, 5)])
    p.add_argument("--max_dte", type=int, default=14)
    p.add_argument("--max_spread_pct", type=float, default=0.15)
    p.add_argument("--min_rr", type=float, default=0.8)
    p.add_argument("--out", type=Path, default=Path("trades.csv"))
    return p.parse_args(argv)


def main(argv: List[str] | None = None):
    args = _parse_cli(argv or sys.argv[1:])

    bt = Backtester(
        signals_path=args.signals,
        orats_path=args.orats,
        exit_path=args.exit,
        deltas=args.deltas,
        max_spread_pct=args.max_spread_pct,
        min_rr=args.min_rr,
        max_dte=args.max_dte,
    )

    trades = bt.run()
    if not trades.empty:
        trades.to_csv(args.out, index=False)
        LOGGER.info("Trade log saved to %s", args.out)


if __name__ == "__main__":
    main()
