#!/usr/bin/env python3
"""
split_csv.py

Split a CSV file into multiple files of N rows each (excluding the header).
Each output file will include the header. The last file may have fewer rows.

Usage:
    python split_csv.py --input large_file.csv --rows 200 \
                        --prefix chunk_ --outdir ./output_folder
"""

import csv
import os
import argparse

def split_csv(input_path: str, rows_per_file: int, prefix: str, outdir: str):
    # Create output directory if it doesn't exist
    os.makedirs(outdir, exist_ok=True)

    with open(input_path, newline='', encoding='utf-8') as infile:
        reader = csv.reader(infile)
        header = next(reader)  # grab header row

        file_count = 1
        rows_written = 0
        out_file = None
        writer = None

        def open_new_file(idx):
            filename = f"{prefix}{idx:03d}.csv"
            path = os.path.join(outdir, filename)
            f = open(path, 'w', newline='', encoding='utf-8')
            w = csv.writer(f)
            w.writerow(header)
            print(f"Creating {path}")
            return f, w

        # start first chunk
        out_file, writer = open_new_file(file_count)

        for row in reader:
            if rows_written >= rows_per_file:
                out_file.close()
                file_count += 1
                rows_written = 0
                out_file, writer = open_new_file(file_count)

            writer.writerow(row)
            rows_written += 1

        # close the final file
        if out_file:
            out_file.close()

        print(f"Done! Created {file_count} file(s) in '{outdir}'.")

def main():
    parser = argparse.ArgumentParser(
        description="Split a CSV into multiple smaller CSVs.")
    parser.add_argument(
        "--input", "-i", required=True,
        help="Path to the input CSV file."
    )
    parser.add_argument(
        "--rows", "-r", type=int, default=200,
        help="Number of data rows per output file (default: 200)."
    )
    parser.add_argument(
        "--prefix", "-p", default="part_",
        help="Prefix for output filenames (default: 'part_')."
    )
    parser.add_argument(
        "--outdir", "-o", default="./signal-files",
        help="Directory to write chunk files into (default: current directory)."
    )
    args = parser.parse_args()

    split_csv(args.input, args.rows, args.prefix, args.outdir)

if __name__ == "__main__":
    main()
