#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
orats_backtest.py   ·   Weekly debit-spread tester (extended)

Implements a one-trade-per-signal weekly debit vertical spread backtest,
using signal 'trend' to choose call vs put, with full inline math
and detailed trade.csv output.

Definitions:
  • Delta (Δ): ∂OptionPrice/∂StockPrice. We target |Δ| values
    (e.g. 0.10, 0.25, 0.40) by selecting the option whose absolute
    delta is closest to the target.
  • Bid-Ask Spread: SP = askPrice − bidPrice. This is the per-contract
    transaction cost; we enforce SP ≤ max_spread_pct × bidPrice.
  • Width: W = |K_S − K_L|, the difference in strikes; the maximum
    possible intrinsic value if the option finishes in-the-money.
  • Debit: D = askPrice_long − bidPrice_short; the net premium paid
    to open the spread.
  • Risk-Reward (RR): RR = (W − D) / D; how much width you buy per unit
    of debit. Must satisfy RR ≥ min_rr.
  • Entry Stock Price: S_entry = stockPrice at entry snapshot.
  • Exit Stock Price: S_exit  = stockPrice at exit snapshot.
  • Mid Prices at Exit: M_L_exit = (bid_L_exit + ask_L_exit)/2, etc.
  • PnL = (M_L_exit − M_S_exit) − D.

Trade lifecycle:
 1. Read signals.csv (must have columns [time-col, ticker, trend]),
    where trend ∈ {"Bullish","Bearish"}.
 2. For each signal: parse ts → UTC, uppercase ticker, read trend.
 3. Map timestamp → that week’s Friday expiry in ET.
 4. From entry/exit files, filter to ticker+expiry and load:
      [ticker, expirDate, strike, delta, bidPrice, askPrice, stockPrice, quoteDate, optionType].
 5. ENTRY: pick the last minute ≤ signal time.
    EXIT: pick the very last minute on Friday.
 6. Determine side: Bullish→call, Bearish→put.
 7. For each delta_target:
      a. Find K_L whose |Δ| is closest to target.
      b. Find K_S at the next strike (higher for call, lower for put).
      c. Compute W, D, RR, SP_long, SP_short.
      d. Enforce SP_long ≤ pct, SP_short ≤ pct, RR ≥ min_rr.
      e. Compute exit mids M_L_exit, M_S_exit, S_entry, S_exit, PnL.
 8. Among candidates for that signal, pick the one with highest RR.
 9. Write one row per signal to trades.csv with all intermediates,
    plus summary metrics to report.json.

Usage:
  python orats_backtest.py --signals signals.csv --time-col t --signals-tz UTC \
       --input-dir output --entry-file entry.csv.gz --exit-file exit.csv.gz \
       --delta-targets 0.10,0.25,0.40 --max-spread-pct 0.15 --min-rr 0.8 \
       --outdir results --log-level INFO
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List

import pandas as pd
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
LOG = logging.getLogger("orats_bt")


def parse_args() -> argparse.Namespace:
    """Define CLI flags (unchanged interface)."""
    p = argparse.ArgumentParser("ORATS debit-spread back-tester (extended)")
    p.add_argument("--signals",      type=Path, required=True,
                   help="CSV with signal timestamps, ticker, and trend")
    p.add_argument("--time-col",     default="t",
                   help="Column name for timestamp in signals CSV")
    p.add_argument("--signals-tz",   default="UTC", choices=["UTC", "ET"],
                   help="Assume naïve timestamps are UTC or ET")
    p.add_argument("--input-dir",    type=Path, default=Path("./output"),
                   help="Dir holding entry.csv.gz & exit.csv.gz")
    p.add_argument("--entry-file",   type=Path, default=Path("entry.csv.gz"),
                   help="ORATS minute snapshots for ENTRY pricing")
    p.add_argument("--exit-file",    type=Path, default=Path("exit.csv.gz"),
                   help="ORATS minute snapshots for EXIT pricing")
    p.add_argument("--outdir",       type=Path, default=Path("results"),
                   help="Where to write results/trades.csv and report.json")
    p.add_argument("--delta-targets",default="0.10,0.25,0.40",
                   help="Comma-separated target deltas")
    p.add_argument("--max-spread-pct",type=float, default=0.15,
                   help="Max bid-ask spread ≤ this fraction of bid price")
    p.add_argument("--min-rr",       type=float, default=0.8,
                   help="Min risk-reward ratio")
    p.add_argument("--log-level",    default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                   help="Logging verbosity")
    return p.parse_args()


def floor_minute(ts: datetime) -> datetime:
    """Drop seconds & microseconds to align to minute grid."""
    return ts.replace(second=0, microsecond=0)


def next_friday(ts_et: datetime) -> datetime:
    """
    Given any ET datetime, return the 00:00 ET of Friday in that Mon–Fri week.
    days_to_friday = (4 - weekday) % 7
    """
    days_to_friday = (4 - ts_et.weekday()) % 7
    friday = ts_et + timedelta(days=days_to_friday)
    return friday.replace(hour=0, minute=0, second=0, microsecond=0)


def parse_ts(val, default_tz: str) -> datetime:
    """
    Parse an ISO timestamp into tz-aware datetime.
    If naive, localize to UTC or ET by default_tz.
    """
    dt = pd.to_datetime(val)
    if dt.tzinfo is None:
        tz = timezone.utc if default_tz == "UTC" else ET
        dt = dt.tz_localize(tz)
    return dt


# We include stockPrice here to record underlying at each snap.
ORATS_KEEP = [
    "ticker","expirDate","strike","delta",
    "callBidPrice","callAskPrice","putBidPrice","putAskPrice",
    "stockPrice","quoteDate"
]


def load_orats(path: Path, tickers: list[str], expiries: list[str]) -> pd.DataFrame:
    """
    Load ORATS one-minute CSV, filter to our tickers+expiries, then split:
    - Calls: bidPrice=callBidPrice, askPrice=callAskPrice
    - Puts:  bidPrice=putBidPrice,  askPrice=putAskPrice, delta-=1
    Return unified table with columns:
    [ticker, expirDate, strike, delta, quoteDate,
     bidPrice, askPrice, stockPrice, optionType]
    """
    LOG.info("Loading %s …", path.name)
    df = pd.read_csv(
        path,
        usecols=ORATS_KEEP,
        parse_dates=["quoteDate"],
        dtype={"ticker":"category","expirDate":"string"}
    )
    df["quoteDate"] = pd.to_datetime(df["quoteDate"], utc=True)
    df = df[df["ticker"].isin(tickers) & df["expirDate"].isin(expiries)]

    # Calls
    calls = df.rename(columns={
        "callBidPrice":"bidPrice","callAskPrice":"askPrice"
    })[[
        "ticker","expirDate","strike","delta","quoteDate",
        "bidPrice","askPrice","stockPrice"
    ]].copy()
    calls["optionType"] = "call"

    # Puts
    puts = df.rename(columns={
        "putBidPrice":"bidPrice","putAskPrice":"askPrice"
    })[[
        "ticker","expirDate","strike","delta","quoteDate",
        "bidPrice","askPrice","stockPrice"
    ]].copy()
    puts["optionType"] = "put"
    puts["delta"] = puts["delta"] - 1.0  # make negative

    return pd.concat([calls, puts], ignore_index=True)


def load_signals(path: Path, col: str, tz_flag: str) -> pd.DataFrame:
    """
    Read signals CSV. Must include columns: [col, ticker, trend].
    Parse timestamps → UTC, uppercase tickers, preserve trend.
    """
    LOG.info("Reading signals …")
    df = pd.read_csv(path)
    if "ticker" not in df.columns:
        sys.exit("signals file must have a 'ticker' column")
    if "trend" not in df.columns:
        sys.exit("signals file must have a 'trend' column")

    ts_list = [parse_ts(x, tz_flag).tz_convert("UTC") for x in df[col]]
    return pd.DataFrame({
        "ts_utc": ts_list,
        "ticker": df["ticker"].str.upper(),
        "trend":  df["trend"].str.capitalize()  # "Bullish" or "Bearish"
    })


def run(cfg: argparse.Namespace) -> None:
    """Main engine: load data, backtest, write outputs."""
    cfg.outdir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=getattr(logging, cfg.log_level),
                        format="%(asctime)s [%(levelname)s] %(message)s")

    # Load signals, determine tickers & expiries
    sig = load_signals(cfg.signals, cfg.time_col, cfg.signals_tz)
    tickers = sig["ticker"].unique().tolist()
    expiries = sorted({
        next_friday(ts.astimezone(ET)).date().isoformat()
        for ts in sig["ts_utc"]
    })

    # Load entry/exit snapshots
    entry = load_orats(cfg.input_dir / cfg.entry_file, tickers, expiries)
    exit_ = load_orats(cfg.input_dir / cfg.exit_file,  tickers, expiries)

    targets = [float(x) for x in cfg.delta_targets.split(",")]
    all_trades: List[dict] = []

    # Iterate signals
    for ts_utc, tkr, trend in sig.itertuples(index=False):
        # Determine expiry Friday
        expiry = next_friday(ts_utc.astimezone(ET)).date().isoformat()

        ent_all = entry[(entry["ticker"]==tkr)&(entry["expirDate"]==expiry)]
        ext_all = exit_[(exit_["ticker"]==tkr)&(exit_["expirDate"]==expiry)]
        if ent_all.empty or ext_all.empty:
            continue

        # ENTRY snap = last minute ≤ signal
        ent_time = ent_all[ent_all["quoteDate"] <= floor_minute(ts_utc)]["quoteDate"].max()
        if pd.isna(ent_time):
            continue
        ent_snap = ent_all[ent_all["quoteDate"] == ent_time]

        # EXIT snap = final minute on Friday
        ext_time = ext_all["quoteDate"].max()
        ext_snap = ext_all[ext_all["quoteDate"] == ext_time]

        # Decide side from signal trend
        side = "call" if trend=="Bullish" else "put"

        # Collect candidates by delta_target
        cand: List[dict] = []
        for tgt in targets:
            # Long leg: option whose |delta| is closest to tgt
            long_row = (
                ent_snap[ent_snap["optionType"]==side]
                .assign(dist=(ent_snap["delta"].abs()-tgt).abs())
                .sort_values("dist").head(1).squeeze()
            )
            K_L = long_row.strike
            Δ_L = long_row.delta  # actual delta

            # Short leg: next strike (higher for call, lower for put)
            if side=="call":
                shorts = ent_snap[(ent_snap["optionType"]==side)&(ent_snap["strike"]>K_L)].sort_values("strike")
            else:
                shorts = ent_snap[(ent_snap["optionType"]==side)&(ent_snap["strike"]<K_L)].sort_values("strike", ascending=False)
            if shorts.empty:
                continue
            short_row = shorts.head(1).squeeze()
            K_S = short_row.strike
            Δ_S = short_row.delta

            # Underlying at entry
            S_entry = float(long_row.stockPrice)

            # Compute width, debit, RR
            W  = abs(K_S - K_L)
            ask_L = long_row.askPrice
            bid_S = short_row.bidPrice
            D  = ask_L - bid_S
            RR = (W - D) / D

            # Compute bid-ask spreads & spread pct
            SP_L = long_row.askPrice - long_row.bidPrice
            SP_S = short_row.askPrice - short_row.bidPrice
            SP_PCT_L = SP_L / long_row.bidPrice
            SP_PCT_S = SP_S / short_row.bidPrice

            # Enforce criteria
            if SP_PCT_L > cfg.max_spread_pct: continue
            if SP_PCT_S > cfg.max_spread_pct: continue
            if RR < cfg.min_rr:              continue

            # Exit mid-prices
            def mid(df: pd.DataFrame, K: float) -> float|None:
                row = df[df["strike"]==K]
                if row.empty: return None
                return float((row.bidPrice.values[0] + row.askPrice.values[0]) / 2)

            M_L_exit = mid(ext_snap, K_L)
            M_S_exit = mid(ext_snap, K_S)
            if M_L_exit is None or M_S_exit is None:
                continue

            # Underlying at exit
            S_exit = float(ext_snap["stockPrice"].iloc[0])

            # PnL
            pnl = (M_L_exit - M_S_exit) - D

            # Record candidate
            cand.append({
                "timestamp":       ts_utc.isoformat(),
                "ticker":          tkr,
                "trend":           trend,
                "side":            side,
                "delta_target":    tgt,
                "delta_long":      round(Δ_L,4),
                "delta_short":     round(Δ_S,4),
                "strike_long":     float(K_L),
                "strike_short":    float(K_S),
                "S_entry":         round(S_entry,4),
                "S_exit":          round(S_exit,4),
                "width":           round(W,4),
                "debit":           round(D,4),
                "RR":              round(RR,4),
                "spread_long":     round(SP_L,4),
                "spread_short":    round(SP_S,4),
                "spread_pct_long": round(SP_PCT_L,4),
                "spread_pct_short":round(SP_PCT_S,4),
                "mid_long_exit":   round(M_L_exit,4),
                "mid_short_exit":  round(M_S_exit,4),
                "pnl":             round(pnl,4),
            })

        # Pick the best candidate by highest RR
        if cand:
            best = max(cand, key=lambda x: x["RR"])
            # LOG.info("SELECTED TRADE %s", best)
            all_trades.append(best)

    # No trades?
    if not all_trades:
        LOG.warning("No trades produced.")
        return

    # Write trades.csv
    trades_df = pd.DataFrame(all_trades).sort_values("timestamp")
    trades_df.to_csv(cfg.outdir / "trades.csv", index=False)

    # Summary metrics → report.json
    hit   = (trades_df.pnl > 0).mean()
    mu, sigma = trades_df.pnl.mean(), trades_df.pnl.std(ddof=0)
    sharpe = math.nan if sigma==0 else (mu/sigma)*math.sqrt(52)
    eq     = trades_df.pnl.cumsum()
    max_dd = (eq.cummax() - eq).max()

    (cfg.outdir / "report.json").write_text(
        json.dumps({
            "total_trades":  len(trades_df),
            "hit_rate":      float(hit),
            "mean_pnl":      float(mu),
            "std_pnl":       float(sigma),
            "sharpe":        float(sharpe),
            "max_drawdown":  float(max_dd),
        }, indent=2)
    )
    LOG.info("Run complete: %d trades", len(trades_df))


if __name__ == "__main__":
    run(parse_args())
