#!/usr/bin/env python3
import os

import click
from datetime import datetime
from pathlib import Path

from src.ingestion.analytics import run_sanity_check
from src.ingestion.ingestion import APIError, get_items, save_to_parquet

TMP_FOLDER = Path(__file__).parent.parent.parent / "tmp"
API_ENDPOINT = "http://127.0.0.1:8000/".rstrip("/")
PAGE_SIZE = 30
PARQUET_ROW_GROUP_SIZE = 500


@click.group()
def ingest():
    pass


@ingest.command()
@click.option(
    "--output-dir",
    default=TMP_FOLDER,
    help="Output directory for data files",
)
def full_ingest(output_dir):
    ingest_ts = f"{datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')}"
    output_path = f"{output_dir}/{ingest_ts}"
    os.makedirs(output_path, exist_ok=True)

    models = ("tracks", "users", "listen_history")

    print(f"Starting full ingest to {output_path}")
    try:
        for model in models:
            save_to_parquet(
                get_items(f"{API_ENDPOINT}/{model}", PAGE_SIZE),
                f"{output_path}/{model}.parquet",
                buffer_size=PARQUET_ROW_GROUP_SIZE,
            )
            print(f"{model} ingestion finished")
    except APIError as e:
        print(
            f"Failed to ingest data: {e}, status code {e.status_code}, detail {e.error_body}"
        )


def get_most_recent_directory(base_dir="tmp"):
    dirs = [d for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d))]
    dir_times = [(d, datetime.strptime(d, "%Y-%m-%d_%H-%M-%S")) for d in dirs]
    most_recent_dir = max(dir_times, key=lambda x: x[1])[0]
    return os.path.join(base_dir, most_recent_dir)


@ingest.command()
@click.argument(
    "full_dump_dir", default=get_most_recent_directory, type=click.Path(exists=True)
)
def sanity_check(full_dump_dir: str):
    print(f"Starting sanity check on data in {full_dump_dir}")
    non_matching_dim_data = run_sanity_check(full_dump_dir)

    if non_matching_dim_data.users > 0 or non_matching_dim_data.tracks > 0:
        print(
            f"Sanity check failed, {non_matching_dim_data.users} non-matching users and {non_matching_dim_data.tracks} non-matching tracks found"
        )
    else:
        print("Sanity check passed, all data is consistent")


if __name__ == "__main__":
    ingest()
