# backtest_report.py
"""
Clean performance report for debit-option trades.

Key features
------------
* **Single source of truth** – event-driven cash-flow equity curve.
"""

from __future__ import annotations
import pandas as pd
import numpy as np

# ---------------------------------------------------------------------
def option_trade_report(trades: pd.DataFrame,
                        initial_capital: float = 100.0,
                        freq: str = "D",
                        risk_free_rate: float = 0.0
                        ) -> pd.Series:
    """
    Build a performance summary for *debit* option spreads.

    Parameters
    ----------
    trades : DataFrame
        Must contain at least:
            'quote_ts'  – entry timestamp  (UTC)
            'expir'     – exit timestamp   (UTC)
            'debit'     – price paid at entry  (positive number)
            'pnl'       – realised PnL at exit (payoff-debit)
    initial_capital : float, default 100
    freq : str, default 'D'
        Resample frequency for equity curve.
    risk_free_rate : float, default 0
        Annual risk-free rate for Sharpe/Sortino.

    Returns
    -------
    pd.Series
        Human-readable performance table.
    """
    df = trades.copy()
    df["entry_ts"] = pd.to_datetime(df["quote_ts"], utc=True)
    df["exit_ts"]  = pd.to_datetime(df["expir"],    utc=True)

    # -----------------------------------------------------------------
    # 1) Build cash-flow table  (−debit at entry  | +debit+pnl at exit)
    # -----------------------------------------------------------------
    cf_in  = df[["entry_ts", "debit"]].rename(columns={"entry_ts": "ts"})
    cf_in["cash"] = -cf_in["debit"]

    cf_out = df[["exit_ts", "debit", "pnl"]].rename(columns={"exit_ts": "ts"})
    cf_out["cash"] = cf_out["debit"] + cf_out["pnl"]

    cash_flows = (
        pd.concat([cf_in[["ts", "cash"]], cf_out[["ts", "cash"]]])
          .groupby("ts", as_index=True)["cash"]
          .sum()
          .sort_index()
    )

    # -----------------------------------------------------------------
    # 2) Equity curve from cumulative cash-flows
    # -----------------------------------------------------------------
    equity = (cash_flows.cumsum() + initial_capital).resample(freq).last().ffill()

    # -----------------------------------------------------------------
    # 3) Daily (or resampled) returns & risk metrics
    # -----------------------------------------------------------------
    daily_ret  = equity.pct_change().dropna()
    running_max = equity.cummax()
    drawdown    = equity - running_max
    dd_pct      = drawdown / running_max * 100
    max_dd_pct  = dd_pct.min()
    in_dd       = drawdown < 0
    dd_groups   = (~in_dd).cumsum()
    dd_periods  = (
        pd.DataFrame({"ts": equity.index, "in_dd": in_dd, "grp": dd_groups})
          .query("in_dd")
          .groupby("grp")["ts"]
          .agg(start="first", end="last")
          .assign(duration=lambda x: x["end"] - x["start"])
    )
    max_dd_dur = dd_periods["duration"].max()

    # -----------------------------------------------------------------
    # 4) Trade-level stats
    # -----------------------------------------------------------------
    closed = df["exit_ts"].notna()
    wins   = df.loc[closed & (df["pnl"] > 0)]
    losses = df.loc[closed & (df["pnl"] <= 0)]

    win_rate = len(wins)/closed.sum()*100 if closed.sum() else np.nan
    rets     = df.loc[closed, "pnl"] / df.loc[closed, "debit"]
    best, worst = rets.max()*100, rets.min()*100
    avg_win  = rets[rets>0].mean()*100
    avg_lo   = rets[rets<=0].mean()*100
    payoff   = avg_win / -avg_lo if avg_lo < 0 else np.nan
    pfactor  = wins["pnl"].sum() / -losses["pnl"].sum() if len(losses) else np.nan
    expectancy = win_rate/100*avg_win + (1-win_rate/100)*avg_lo

    # -----------------------------------------------------------------
    # 5) Annualised risk/return ratios
    # -----------------------------------------------------------------
    years   = (equity.index[-1] - equity.index[0]).days / 365.25
    start_val, end_val = equity.iloc[0], equity.iloc[-1]
    cagr    = (end_val/start_val)**(1/years) - 1 if years > 0 else np.nan
    ann_vol = daily_ret.std() * np.sqrt(252)
    sharpe  = (cagr - risk_free_rate) / ann_vol if ann_vol else np.nan
    calmar  = cagr / abs(max_dd_pct/100) if max_dd_pct else np.nan
    neg_only = daily_ret[daily_ret < 0]
    sortino = ((cagr - risk_free_rate) /
               (neg_only.std()*np.sqrt(252))) if not neg_only.empty else np.nan
    pos_a = daily_ret[daily_ret > 0].sum()
    neg_a = -daily_ret[daily_ret < 0].sum()
    omega = pos_a/neg_a if neg_a else np.nan

    # -----------------------------------------------------------------
    # 6) Assemble results
    # -----------------------------------------------------------------
    stats = pd.Series({
        "Start": equity.index[0],
        "End":   equity.index[-1],
        "Period": equity.index[-1] - equity.index[0],
        "Start Value": start_val,
        "End Value":   end_val,
        "Total Return [%]": (end_val/start_val - 1) * 100,
        "Max Drawdown [%]": max_dd_pct,
        "Max DD Duration":  max_dd_dur,
        "Sharpe":  sharpe,
        "Sortino": sortino,
        "Calmar":  calmar,
        "Omega":   omega,
        "Total Trades":      len(df),
        "Closed Trades":     closed.sum(),
        "Open Trades":       len(df) - closed.sum(),
        "Win Rate [%]":      win_rate,
        "Best Trade [%]":    best,
        "Worst Trade [%]":   worst,
        "Avg Win [%]":       avg_win,
        "Avg Loss [%]":      avg_lo,
        "Payoff":            payoff,
        "Profit Factor":     pfactor,
        "Expectancy [%]":    expectancy,
    })

    return stats
