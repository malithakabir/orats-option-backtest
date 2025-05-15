# ORATS Weekly Debit‐Spread Backtesting

This repository contains two main scripts:

1. **Data Preparation** (`prepare_orats_data_v5.py`):  
   Fetches the exact ORATS one‐minute snapshots you need for each signal—both at the signal time (“entry”) and at the final minute on the next Friday (“exit”).

2. **Backtester** (`orats_backtest_v6.py`):  
   Runs a one‐trade‐per‐signal weekly debit vertical‐spread backtest, choosing call vs. put based on your signal’s “trend”, targeting specific option deltas, and enforcing bid‐ask spread & risk‐reward criteria.

---

## Directory Structure

```
.
├── cred.json
├── signals.csv
├── requirements.txt
├── prepare_orats_data_v5.py
├── orats_backtest_v6.py
├── output/
│   ├── entry.csv.gz
│   └── exit.csv.gz
└── results/
    ├── trades.csv
    └── report.json
```

## Prerequisites

- **Python** ≥ 3.9  
- Install dependencies:
  ```bash
  pip install -r requirements.txt
  ```
- **Wasabi S3 credentials** in a JSON file (e.g. `cred.json`) with keys:
  ```json
  {
    "ACCESS_KEY": "…",
    "SECRET_KEY": "…",
    "REGION":    "…"
  }
  ```
- **Signal CSV** (e.g. `signals.csv`), **must** include columns:
  - `t` (ISO-format timestamp)
  - `ticker` (e.g. `AAPL`, `FANG`)
  - `trend` (`Bullish` or `Bearish`)

---

## Step 1: Fetch Entry & Exit Snapshots

```bash
python prepare_orats_data_v5.py \
  --cred    cred.json \
  --signals signals.csv \
  --out-dir output
```

1. **Read signals** (`signals.csv`) and parse each timestamp in UTC by default.  
2. **Convert** each timestamp to ORATS timezone (`America/New_York`) to match snapshot filenames (`one-minute/YYYYMMDD/HHMM…`).  
3. **Build two task lists**:  
   - **Entry tasks**: snapshot at the exact minute of each signal.  
   - **Exit tasks** : snapshot on the next Friday at the same clock‐time.  
4. **List S3 objects** under `one-minute/YYYYMMDD/…`, match filenames by `HHMM`, download & filter by ticker (via S3 Select when available, else full download).  
5. **Write** two compressed CSVs under `output/`:  
   - `entry.csv.gz` (one‐minute data at entry times)  
   - `exit.csv.gz`  (one‐minute data at exit times)  

Progress will look like:

```
Entry: 100%|████| 8/8 [02:22…]  
15:25:19 [INFO] ✅  Entry fetch complete (8 snapshots)  
Exit : 100%|████| 8/8 [04:11…]  
15:29:32 [INFO] ✅  Exit  fetch complete (8 snapshots)
```

---

## Step 2: Run the Backtest

```bash
python orats_backtest_v6.py \
  --signals        signals.csv \
  --delta-targets  "0.40"
```

1. **Load** `signals.csv`, parse `t` → UTC, uppercase tickers, read `trend`.  
2. **Determine** for each signal the expiry date (that week’s Friday in ET).  
3. **Load & filter** the two snapshot files:
   - `output/entry.csv.gz`  
   - `output/exit.csv.gz`  
   to only the tickers & expiries you need.  
4. **For each signal**:
   - **Entry snapshot**: last minute ≤ signal time.  
   - **Exit snapshot** : final minute on expiry Friday.  
   - **Side** = call if `trend=="Bullish"`, else put.  
   - **For each delta‐target** (here just 0.40):
     1. Pick the contract whose |Δ| is closest to the target (long leg).  
     2. Pick the next strike (short leg).  
     3. Compute:
        - **Width** = W = |K_S – K_L|  
        - **Debit** = D = ask_L – bid_S  
        - **RR** = (W − D) / D  
        - **Bid-ask spreads** & spread % on each leg  
     4. Enforce:
        - Spread % ≤ `max_spread_pct` (default 15%)  
        - RR ≥ `min_rr` (default 0.8)  
     5. Compute exit mid‐prices, underlying prices, and **P&L**.  
   - **Select** the candidate with highest RR.  
5. **Write outputs** under `results/`:
   - `trades.csv` (one row per signal, with all intermediate math)  
   - `report.json` (summary metrics: total trades, hit rate, mean PnL, std PnL, Sharpe, max drawdown)  

Example log:

```
2025-05-15 15:30:54,528 [INFO] Reading signals …  
2025-05-15 15:30:54,889 [INFO] Loading entry.csv.gz …  
2025-05-15 15:30:56,090 [INFO] Loading exit.csv.gz …  
2025-05-15 15:30:58,355 [INFO] Run complete: 3 trades
```

---

## Configuration & Flags

### `prepare_orats_data_v5.py`
| Flag              | Description                                             | Default                   |
|-------------------|---------------------------------------------------------|---------------------------|
| `--signals`       | CSV with `ticker` + datetime column                     | —                         |
| `--time-col`      | Name of the datetime column                             | `t`                       |
| `--cred`          | Path to Wasabi cred.json                                | `cred.json`               |
| `--bucket`        | Wasabi S3 bucket name                                   | `nufintech-orats`         |
| `--out-dir`       | Directory for output files                              | `out`                     |
| `--entry-file`    | Filename for entry data                                 | `entry.csv.gz`            |
| `--exit-file`     | Filename for exit data                                  | `exit.csv.gz`             |
| `--signal-tz`     | IANA timezone of signal timestamps                      | `UTC`                     |
| `--orats-tz`      | IANA timezone of ORATS data                             | `America/New_York`        |
| `--workers`       | Parallel download threads                               | `16`                      |
| `--use-select`    | Enable S3 Select filtering                              | *off*                     |
| `--max-retries`   | Retry count on SSL/network errors                       | `3`                       |

### `orats_backtest_v6.py`
| Flag               | Description                                                                                    | Default                       |
|--------------------|------------------------------------------------------------------------------------------------|-------------------------------|
| `--signals`        | CSV with `t`, `ticker`, `trend`                                                                | —                             |
| `--time-col`       | Timestamp column in signals CSV                                                                | `t`                           |
| `--signals-tz`     | Timezone of naive timestamps (`UTC` or `ET`)                                                   | `UTC`                         |
| `--input-dir`      | Directory holding `entry.csv.gz` & `exit.csv.gz`                                               | `./output`                    |
| `--entry-file`     | Entry snapshot file name                                                                       | `entry.csv.gz`                |
| `--exit-file`      | Exit snapshot file name                                                                        | `exit.csv.gz`                 |
| `--delta-targets`  | Comma-separated list of target deltas (e.g. `0.10,0.25,0.40`)                                  | `0.10,0.25,0.40`              |
| `--max-spread-pct` | Maximum allowed bid-ask spread as fraction of bid price                                        | `0.15`                        |
| `--min-rr`         | Minimum required risk-reward ratio                                                             | `0.8`                         |
| `--outdir`         | Output directory for `results/trades.csv` & `results/report.json`                              | `./results`                   |
| `--log-level`      | Logging verbosity (`DEBUG`, `INFO`, `WARNING`, `ERROR`)                                        | `INFO`                        |

---
