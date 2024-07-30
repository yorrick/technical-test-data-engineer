from dataclasses import dataclass
import pyarrow.parquet as pq
import pandas as pd


@dataclass
class NonMatchingDimensionData:
    users: int
    tracks: int


def run_sanity_check(full_dump_dir: str) -> NonMatchingDimensionData:
    """
    Check if the data in the full dump directory is consistent
    Only works for small datasets, as it loads all data into memory
    For production use, this should be done in a distributed manner (using Athena, Spark, etc.)
    """
    users = pq.read_table(f"{full_dump_dir}/users.parquet").to_pandas()
    tracks = pq.read_table(f"{full_dump_dir}/tracks.parquet").to_pandas()
    listen_history = pq.read_table(
        f"{full_dump_dir}/listen_history.parquet"
    ).to_pandas()

    # merge facts table listen_history against dimensions tables users and tracks
    non_matching_users = int(
        pd.merge(listen_history, users, left_on="user_id", right_on="id", how="left")[
            "id"
        ]
        .isna()
        .sum()
    )
    non_matching_tracks = int(
        pd.merge(
            listen_history.explode("items").rename(columns={"items": "item_id"}),
            tracks,
            left_on="item_id",
            right_on="id",
            how="left",
        )["id"]
        .isna()
        .sum()
    )

    return NonMatchingDimensionData(non_matching_users, non_matching_tracks)
