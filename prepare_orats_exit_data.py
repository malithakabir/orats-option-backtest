#!/usr/bin/env python3
"""
prepare_orats_exit_data.py
─────────────────────────

Fetch the last-minute ORATS one-minute snapshots for every Friday, extract only the exit fields,
and compile them into a single compressed CSV.

Key steps:
 1. Connect to Wasabi S3 using provided credentials.
 2. List all date folders under `one-minute/`, identify those that fall on Friday.
 3. For each Friday folder, list available snapshot files and select the last one (highest HHMM).
 4. Download (or S3-Select) each chosen file, parse with Polars, filter for required fields,
    cast columns to string for consistency, drop duplicates per ticker.
 5. Write all results into a single compressed CSV at the path given by `--exit-file`.

Usage:
    python prepare_orats_exit_data.py \
      --cred cred.json \
      --bucket nufintech-orats \
      --out-dir output \
      --exit-file exit.csv.gz \
      --orats-tz America/New_York \
      --workers 16 \
      --use-select \
      --max-retries 3
"""
import argparse
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
from botocore.config import Config
from botocore.exceptions import ClientError, SSLError
from tqdm import tqdm
import urllib3.exceptions

# -- Configuration -------------------------------------------------------------
MAX_POOL_CONNECTION = 50  # boto3 max_pool_connections

# -- Logging setup ------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("prepare_orats_exit")

EXIT_COLUMNS = ["ticker", "stockPrice", "snapShotDate", "snapShotEstTime", "spotPrice", "quoteDate"]


def list_friday_folders(s3, bucket: str) -> list[str]:
    """
    Return all YYYYMMDD folder names under `one-minute/` that are Fridays.
    """
    paginator = s3.get_paginator("list_objects_v2")
    fridays = []
    for page in paginator.paginate(Bucket=bucket, Prefix="one-minute/", Delimiter="/"):
        for pref in page.get("CommonPrefixes", []):
            folder = pref["Prefix"].strip("/").split("/")[1]  # YYYYMMDD
            try:
                date_obj = dt.date.fromisoformat(f"{folder[:4]}-{folder[4:6]}-{folder[6:8]}")
            except ValueError:
                continue
            if date_obj.weekday() == 4:  # Friday
                fridays.append(folder)
    return fridays


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


def extract_exit_chunks(
    s3,
    bucket: str,
    key: str,
    use_select: bool,
    max_retries: int
) -> str:
    """
    Download one CSV.GZ file (via S3 Select if enabled), parse it,
    select required exit columns, cast to string, drop duplicates per ticker,
    and return CSV text. Raises on critical failures to skip file.
    """
    raw = None
    # Download with retry logic
    for attempt in range(1, max_retries + 1):
        try:
            if use_select:
                resp = s3.select_object_content(
                    Bucket=bucket,
                    Key=key,
                    ExpressionType="SQL",
                    Expression="SELECT * FROM S3Object s",
                    InputSerialization={"CSV": {"FileHeaderInfo": "USE"}, "CompressionType": "GZIP"},
                    OutputSerialization={"CSV": {}},
                )
                buf = bytearray()
                for ev in resp["Payload"]:
                    if "Records" in ev:
                        buf.extend(ev["Records"]["Payload"])
                raw = io.BytesIO(buf)
            else:
                data = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
                raw = io.BytesIO(data)
            break
        except (ClientError, SSLError, urllib3.exceptions.ProtocolError) as e:
            log.warning("Retry %d/%d downloading %s: %s", attempt, max_retries, key, e)
            time.sleep(2 ** (attempt - 1))
    if raw is None:
        raise RuntimeError(f"Failed to download {key} after {max_retries} attempts")

    # # Parse CSV
    # try:
    #     with gzip.GzipFile(fileobj=raw) as gz:
    #         df = pl.read_csv(gz)
    # except Exception as e:
    #     log.warning("Skipping %s due to parse error: %s", key, e)
    #     raise

    # Parse CSV, but only load the EXIT_COLUMNS as Utf8, skip any malformed rows
    try:
        with gzip.GzipFile(fileobj=raw) as gz:
            df = pl.read_csv(
                gz,
                # only pull in the columns we need
                columns=EXIT_COLUMNS,
                # force them all to string to dodge any type mis-inference
                dtypes={col: pl.Utf8 for col in EXIT_COLUMNS},
                # look at more rows before guessing schema (optional bump)
                infer_schema_length=10_000,
                # drop any bad rows rather than error out
                ignore_errors=True,
            )
    except Exception as e:
        log.warning("Skipping %s due to parse error: %s", key, e)
        raise

    # Ensure required columns exist
    missing = set(EXIT_COLUMNS) - set(df.columns)
    if missing:
        log.warning("Skipping %s: missing columns %s", key, missing)
        raise RuntimeError(f"Missing columns: {missing}")

    # Cast all exit columns to string to avoid schema mismatches
    df = df.select(EXIT_COLUMNS).with_columns(
        [pl.col(col).cast(pl.Utf8) for col in EXIT_COLUMNS]
    )

    # Dedupe: keep exactly one row per ticker (first occurrence), then sort
    df = df.unique(subset=["ticker"]).sort("ticker")

    # Serialize to CSV text
    try:
        csv_out = df.write_csv()
    except Exception as e:
        log.warning("Failed to serialize CSV for %s: %s", key, e)
        raise

    # Polars.write_csv() may return str or bytes—decode only if bytes.
    if isinstance(csv_out, (bytes, bytearray)):
        return csv_out.decode("utf8")
    return csv_out


def writer_worker(writer: gzip.GzipFile, q: queue.Queue):
    """
    Consume CSV-text items from the queue and append to `writer`.
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


def process_exit_tasks(
    s3,
    bucket: str,
    tasks: list[tuple[str, str]],
    out_path: Path,
    workers: int,
    use_select: bool,
    max_retries: int,
):
    """
    Given a list of (date_folder, key) tasks, fetch and write them
    into a single compressed CSV at out_path with a progress bar.
    """
    log.info("👉 Starting Exit fetch with %d workers", workers)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    gz_writer = gzip.open(out_path, "wt", encoding="utf8", compresslevel=5)
    write_q: queue.Queue = queue.Queue(maxsize=workers * 2)
    t = threading.Thread(target=writer_worker, args=(gz_writer, write_q), daemon=False)
    t.start()

    with ThreadPoolExecutor(max_workers=workers) as exe:
        future_map = {
            exe.submit(extract_exit_chunks, s3, bucket, key, use_select, max_retries): key
            for _, key in tasks
        }
        for fut in tqdm(as_completed(future_map), total=len(future_map), desc="Exit", unit="file"):
            key = future_map[fut]
            try:
                csv_text = fut.result()
                write_q.put((None, csv_text))
            except Exception as e:
                log.error("Error processing %s: %s", key, e)
                # log.error("Skipping file %s due to errors", key)

    write_q.join()
    write_q.put(None)
    t.join()
    log.info("✅  Exit fetch complete (%d snapshots)", len(tasks))


def main():
    ap = argparse.ArgumentParser(description="Build an exit database from ORATS Friday snapshots.")
    ap.add_argument("--cred",        default="cred.json", help="Path to Wasabi cred.json")
    ap.add_argument("--bucket",      default="nufintech-orats", help="Wasabi S3 bucket name")
    ap.add_argument("--out-dir",     default="output", help="Directory to write the exit CSV")
    ap.add_argument("--exit-file",   default="exit.csv.gz", help="Filename for exit CSV")
    ap.add_argument("--orats-tz",    default="America/New_York", help="IANA timezone of ORATS data")
    ap.add_argument("--workers",     type=int, default=16, help="Parallel download threads")
    ap.add_argument("--use-select",  action="store_true", help="Enable S3 Select filtering")
    ap.add_argument("--max-retries", type=int, default=3, help="Retry on SSL/network errors")
    args = ap.parse_args()

    # Initialize S3 client
    cfg = Config(max_pool_connections=MAX_POOL_CONNECTION,
                 retries={"max_attempts": 10, "mode": "adaptive"})
    env = json.load(open(args.cred))
    s3 = boto3.client(
        "s3",
        aws_access_key_id=env["ACCESS_KEY"],
        aws_secret_access_key=env["SECRET_KEY"],
        region_name=env["REGION"],
        endpoint_url=f"https://s3.{env['REGION']}.wasabisys.com",
        config=cfg,
    )

    # Discover Friday folders and their last snapshots
    fridays = list_friday_folders(s3, args.bucket)
    log.info("Found %d Friday folders", len(fridays))
    exit_tasks: list[tuple[str,str]] = []
    # count = 0
    for d in fridays:
        keys = list_keys(s3, args.bucket, d)
        if not keys:
            log.warning("⚠️  no snapshots for %s", d)
            continue
        chosen = sorted(keys)[-1]
        exit_tasks.append((d, chosen))
        # count += 1
        # if count > 2:
        #     break

    # Fetch, extract, and write exit data
    out_path = Path(args.out_dir) / args.exit_file
    process_exit_tasks(
        s3, args.bucket, exit_tasks,
        out_path,
        workers=args.workers,
        use_select=args.use_select,
        max_retries=args.max_retries
    )


if __name__ == "__main__":
    main()
