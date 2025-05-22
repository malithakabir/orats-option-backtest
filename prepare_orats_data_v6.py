#!/usr/bin/env python3
"""
prepare_orats_data_v6.py
────────────────────────

This script fetches exactly the ORATS one-minute snapshots you need based on a
signal file, plus the corresponding next-Friday settlement snapshots.  

Key steps:
 1. Reads a CSV of signals, each with columns:
      • `ticker`         — e.g. FANG, AAPL, etc.
      • a datetime col   — e.g. “t” or whatever you specify via `--time-col`
 2. Parses each timestamp in the user’s signal timezone (`--signal-tz`, default UTC).
 3. Converts that timestamp into the ORATS data timezone (`--orats-tz`, default America/New_York),
    so that the date (YYYYMMDD) and time (HHMM) match the one-minute snapshot filenames on S3.
 4. For each signal, schedules one fetche:
      • **Entry**: the exact snapshot at the converted date and time.
 5. Lists objects under `one-minute/{YYYYMMDD}/` in your Wasabi S3 bucket,
    finds the key whose filename contains the HHMM minute string,
    downloads & filters it (optionally via S3 Select), and returns only the requested tickers.
 6. Retries any SSL/network errors up to `--max-retries` times with exponential backoff.
 7. Writes one compressed CSVs (entry) under `--out-dir`, named by `--entry-file`.
 8. Matches boto3’s `max_pool_connections` to the number of download worker threads,
    ensuring you consume at most as many S3 connections as you have threads.
 9. Uses a small in-memory queue buffer of `workers * 2` to decouple downloaders
    from the single writer thread without opening extra connections.


Usage:
    python prepare_orats_data_v6.py \
      --signals signals.csv \
      --time-col t \
      --cred cred.json \
      --out-dir output \
      --entry-file entry.csv.gz \
      --signal-tz UTC \
      --orats-tz America/New_York \
      --bucket nufintech-orats \
      --workers 16 \
      --use-select \
      --max-retries 3
"""
import argparse
import csv
import gzip
import io
import json
import logging
import threading
import time
import datetime as dt
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import queue

import boto3
import polars as pl
import zoneinfo
from botocore.config import Config
from botocore.exceptions import ClientError, SSLError
from tqdm import tqdm
import urllib3.exceptions

# -- Configuration -------------------------------------------------------------
MAX_POOL_CONNECTION = 50  # boto3 max_pool_connections (default 10)

# -- Logging setup ------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("prepare_signals")



def list_keys(s3, bucket: str, date_folder: str) -> list[str]:
    """
    List all object keys under one-minute/{date_folder}/ in the S3 bucket.
    """
    prefix = f"one-minute/{date_folder}/"
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith("/"):
                keys.append(key)
    return keys


def extract_chunks(
    s3,
    bucket: str,
    key: str,
    tickers: set[str],
    use_select: bool,
    max_retries: int,
) -> dict[str, str]:
    """
    Download one CSV.GZ file from S3 (via S3 Select if enabled),
    parse with Polars, and return a dict mapping ticker -> CSV text (with header).
    Retries full-download on SSL/network errors up to max_retries.
    """
    do_select = use_select and bool(tickers)
    raw = None

    # Attempt S3 Select if requested
    if do_select:
        expr = (
            "SELECT * FROM S3Object s WHERE s.ticker IN ("
            + ",".join(f"'{t}'" for t in tickers)
            + ")"
        )
        try:
            resp = s3.select_object_content(
                Bucket=bucket,
                Key=key,
                ExpressionType="SQL",
                Expression=expr,
                InputSerialization={"CSV": {"FileHeaderInfo": "USE"}, "CompressionType": "GZIP"},
                OutputSerialization={"CSV": {}},
            )
            buf = bytearray()
            for ev in resp["Payload"]:
                if "Records" in ev:
                    buf.extend(ev["Records"]["Payload"])
            if buf:
                raw = io.BytesIO(buf)
            else:
                return {}
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "AccessDenied":
                log.warning("🔒  S3 Select denied on %s — falling back to full download", key)
                do_select = False
            else:
                raise

    # Fallback: full GetObject with retry on SSL/network failure
    if not do_select:
        for attempt in range(1, max_retries + 1):
            try:
                obj = s3.get_object(Bucket=bucket, Key=key)
                # Fully buffer the response into memory before decompressing
                data = obj["Body"].read()
                raw = io.BytesIO(data)
                break
            except (SSLError, urllib3.exceptions.ProtocolError) as e:
                log.warning(
                    "⚠️  SSL/network error fetching %s (attempt %d/%d): %s",
                    key, attempt, max_retries, e
                )
                time.sleep(2 ** (attempt - 1))
        else:
            # All retries failed
            raise

    # Decompress & parse
    
    try:
        with gzip.GzipFile(fileobj=raw) as gz:
            df = pl.read_csv(
                gz,
                infer_schema_length=10_000,
                schema_overrides={"snapShotEstTime": pl.Utf8},
                ignore_errors=True,
                truncate_ragged_lines=True
            )
    except Exception as e:
        log.error("❌  Failed to parse %s: %s", key, e)
        # Optionally wrap and re-raise so the failure still bubbles up:
        raise RuntimeError(f"Error parsing {key}") from e

    if df.height == 0:
        return {}

    # Split out per ticker
    chunks: dict[str, str] = {}
    to_iter = tickers if tickers else set(df["ticker"].unique())
    for tkr in to_iter:
        grp = df.filter(pl.col("ticker") == tkr) if tickers else df
        if grp.height == 0:
            continue
        csv_bytes = grp.write_csv()
        text = (
            csv_bytes.decode("utf8")
            if isinstance(csv_bytes, (bytes, bytearray))
            else csv_bytes
        )
        chunks[tkr] = text

    return chunks


def writer_worker(writer: gzip.GzipFile, q: queue.Queue):
    """
    Consume (ticker, csv_text) items from the queue and append to `writer`.
    Strips the header line after the first write.
    """
    first = True
    while True:
        item = q.get()
        if item is None:
            break
        _, csv_text = item
        if first:
            writer.write(csv_text)
            first = False
        else:
            lines = csv_text.splitlines()
            writer.write("\n".join(lines[1:]) + "\n")
        q.task_done()
    writer.close()


def process_fetch_tasks(
    s3, bucket: str,
    tasks: list[tuple[str, str, set[str]]],
    out_path: Path,
    desc: str,
    workers: int,
    use_select: bool,
    max_retries: int,
):
    """
    Given a list of (date_folder, key, tickers) tasks, fetch and write them
    into a single compressed CSV at out_path with a progress bar.
    """
    log.info("👉 Starting %s fetch with %d workers", desc, workers)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    gz_writer = gzip.open(out_path, "wt", compresslevel=5)
    write_q: queue.Queue = queue.Queue(maxsize=workers * 2)
    t = threading.Thread(target=writer_worker, args=(gz_writer, write_q), daemon=False)
    t.start()

    with ThreadPoolExecutor(max_workers=workers) as exe:
        futures = {
            exe.submit(extract_chunks, s3, bucket, key, tkset, use_select, max_retries): (key, tkset)
            for _, key, tkset in tasks
        }
        for fut in tqdm(as_completed(futures), total=len(futures), desc=desc, unit="file"):
            chunks = fut.result()
            for tkr, txt in chunks.items():
                write_q.put((tkr, txt))

    write_q.join()
    write_q.put(None)
    t.join()
    log.info("✅  %s fetch complete (%d snapshots)", desc, len(tasks))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--signals",      required=True, help="CSV file with 'ticker' and datetime column")
    ap.add_argument("--time-col",     default="t", help="Name of the datetime column in the signals CSV")
    ap.add_argument("--cred",         default="cred.json", help="Path to Wasabi cred.json")
    ap.add_argument("--bucket",       default="nufintech-orats", help="Wasabi S3 bucket name")
    ap.add_argument("--out-dir",      default="output",           help="Directory to write outputs")
    ap.add_argument("--entry-file",   default="entry.csv.gz",  help="Filename for entry CSV")
    ap.add_argument("--signal-tz",    default="UTC",           help="IANA timezone of signal timestamps")
    ap.add_argument("--orats-tz",     default="America/New_York", help="IANA timezone of ORATS data")
    ap.add_argument("--workers",      type=int, default=4,     help="Parallel download threads (default 4)")
    ap.add_argument("--use-select",   action="store_true",     help="Enable S3 Select filtering")
    ap.add_argument("--max-retries",  type=int, default=3,     help="Retry on SSL/network errors")
    args = ap.parse_args()

    sig_zone = zoneinfo.ZoneInfo(args.signal_tz)
    ora_zone = zoneinfo.ZoneInfo(args.orats_tz)

    # S3 client: max_pool_connections
    env = json.load(open(args.cred))
    cfg = Config(max_pool_connections=MAX_POOL_CONNECTION, retries={"max_attempts": 10, "mode": "adaptive"})
    s3 = boto3.client(
        "s3",
        aws_access_key_id=env["ACCESS_KEY"],
        aws_secret_access_key=env["SECRET_KEY"],
        region_name=env["REGION"],
        endpoint_url=f"https://s3.{env['REGION']}.wasabisys.com",
        config=cfg,
    )

    # Read & convert signals
    signals = []
    with open(args.signals, newline="") as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            raw = dt.datetime.fromisoformat(row[args.time_col])
            if raw.tzinfo is None:
                raw = raw.replace(tzinfo=sig_zone)
            ts_et = raw.astimezone(ora_zone)
            signals.append((row["ticker"].strip().upper(), ts_et))

    # Build entry & exit maps
    entry_map = {}
    for tkr, ts in signals:
        d, m = ts.strftime("%Y%m%d"), ts.strftime("%H%M")
        entry_map.setdefault((d, m), set()).add(tkr)

    # Locate S3 keys
    entry_tasks, exit_tasks = [], []
    for (d, m), tkset in entry_map.items():
        keys = list_keys(s3, args.bucket, d)
        suffix = f"{d}{m}.csv.gz"
        matches = [k for k in keys if k.endswith(suffix)]

        if not matches:
            log.warning("⚠️  no snapshot for %s at %s", d, m)
        else:
            if len(matches) > 1:
                # log.warning("⚠️  multiple for %s at %s, taking first", d, m)
                log.warning("⚠️  multiple for %s at %s, taking first\n    %s",
                            d, m, "\n    ".join(matches))
            entry_tasks.append((d, matches[0], tkset))

    # Fetch & write to disk
    out_dir = Path(args.out_dir)
    process_fetch_tasks(
        s3, args.bucket, entry_tasks,
        out_dir / args.entry_file, desc="Entry",
        workers=args.workers,
        use_select=args.use_select,
        max_retries=args.max_retries
    )


if __name__ == "__main__":
    main()
