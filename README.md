# ORATS Weekly Debit‐Spread Backtesting

This repository contains two main scripts:

1. **Entry Data Preparation** (`prepare_orats_data_v6.py`):  
   Fetches the exact ORATS one‐minute snapshots you need for each signal at the signal time (“entry”).

1. **Exit Data Preparation** (`prepare_orats_exit_data.py`)
   Fetches the last snapshot of all the fridays.

1. **Backtester** (`orats_backtest_v12.py`):  
   Runs a one‐trade‐per‐signal weekly debit vertical‐spread backtest, choosing call vs. put based on your signal’s “trend”, targeting specific option deltas, and enforcing bid‐ask spread & risk‐reward criteria.

---

## Directory Structure

```
.
├── report.ipynb
├── cred.json
├── requirements.txt
├── split_files.py
├── prepare_orats_exit_data.py
├── prepare_orats_data_v5.py
├── orats_backtest_v6.py
├── output/
│   ├── part_001_entry.csv.gz
    ├── part_002_entry.csv.gz
│   └── exit.csv.gz
└── results/
    ├── part_001_trades.csv
    ├── part_002_trades.csv
    └── all_trades.csv
└── signal-files/
    ├── part_001.csv
    ├── part_002.csv
    └── part_003.csv
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
  - `signal_id` numeric or str to identify each signal

---

## Step 1: Fetch Entry & Exit Snapshots

```bash
python prepare_orats_data_v6.py \
  --cred    cred.json \
  --signals signal-files/part_001.csv \
  --out-dir output
```

1. **Read signals** (`signal-files/part_001.csv`) and parse each timestamp in UTC by default.  
2. **Convert** each timestamp to ORATS timezone (`America/New_York`) to match snapshot filenames (`one-minute/YYYYMMDD/HHMM…`).  
3. **Entry tasks**: snapshot at the exact minute of each signal.  
4. **Exit tasks** : the last snapshot on the each Friday.  
4. **List S3 objects** under `one-minute/YYYYMMDD/…`, match filenames by `HHMM`, download & filter by ticker (via S3 Select when available, else full download).  
5. **Write** two compressed CSVs under `output/`:  
   - `part_001_entry.csv.gz` (one‐minute data at entry times)  
   - `exit.csv.gz`  (one‐minute data at exit times)  
---

## Configuration & Flags

### `prepare_orats_data_v6.py`
| Flag              | Description                                             | Default                   |
|-------------------|---------------------------------------------------------|---------------------------|
| `--signals`       | CSV with `ticker` + datetime column                     | —                         |
| `--time-col`      | Name of the datetime column                             | `t`                       |
| `--cred`          | Path to Wasabi cred.json                                | `cred.json`               |
| `--bucket`        | Wasabi S3 bucket name                                   | `nufintech-orats`         |
| `--out-dir`       | Directory for output files                              | `out`                     |
| `--entry-file`    | Filename for entry data                                 | `entry.csv.gz`            |
| `--signal-tz`     | IANA timezone of signal timestamps                      | `UTC`                     |
| `--orats-tz`      | IANA timezone of ORATS data                             | `America/New_York`        |
| `--workers`       | Parallel download threads                               | `4`                      |
| `--use-select`    | Enable S3 Select filtering                              | *off*                     |
| `--max-retries`   | Retry count on SSL/network errors                       | `3`                       |

### `orats_backtest_v12.py`
| Flag               | Description                                                                                    | Default                       |
|--------------------|------------------------------------------------------------------------------------------------|-------------------------------|
| `--signals`        | CSV with `t`, `ticker`, `trend`                                                                | —                             |
| `--time-col`       | Timestamp column in signals CSV                                                                | `t`                           |
| `--signals-tz`     | Timezone of naive timestamps (`UTC` or `ET`)                                                   | `UTC`                         |
| `--entry-file`     | Entry snapshot filepath                                                                        | `output/part_001_entry.csv.gz`|
| `--exit-file`      | Exit snapshot filepath                                                                         | `output/exit.csv.gz`          |
| `--delta-targets`  | Comma-separated list of target deltas (e.g. `0.10,0.25,0.40`)                                  | `0.10 to 1.0`                 |
| `--max-spread-pct` | Maximum allowed bid-ask spread as fraction of bid price                                        | `0.15`                        |
| `--min-rr`         | Minimum required risk-reward ratio                                                             | `0.8`                         |
| `--out`            | Output path for `trades.csv`                                                                   | `trades.csv`                  |

---
