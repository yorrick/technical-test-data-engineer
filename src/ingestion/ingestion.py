from typing import Dict, Any, Iterable
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


class APIError(Exception):
    def __init__(self, message, status_code, error_body):
        super().__init__(message)
        self.status_code = status_code
        self.error_body = error_body


def get_items(endpoint: str, page_size: int) -> Iterable[Dict[str, Any]]:
    params = {
        "page": 1,
        "size": page_size,
    }

    max_pages = page_size  # will be updated in while loop

    while params["page"] <= max_pages:
        response = requests.get(endpoint, params=params)

        if response.status_code == 200:
            max_pages = response.json()["pages"]
            for item in response.json()["items"]:
                yield item
            params["page"] += 1
        else:
            raise APIError(
                f"Failed to get data from {endpoint}",
                response.status_code,
                response.json(),
            )


def save_to_parquet(data: Iterable[Dict[str, Any]], output_file: str, buffer_size: int):
    """
    Save data to a parquet file in chunks so we don't load the entire dataset into memory
    """
    first_chunk = True
    writer = None

    for chunk in chunkify(data, buffer_size):
        df = pd.DataFrame(chunk)
        table = pa.Table.from_pandas(df)

        if first_chunk:
            writer = pq.ParquetWriter(output_file, table.schema)
            first_chunk = False

        writer.write_table(table)

    if writer:
        writer.close()


def chunkify(
    iterable: Iterable[Dict[str, Any]], chunk_size: int
) -> Iterable[Iterable[Dict[str, Any]]]:
    buffer = []
    for item in iterable:
        buffer.append(item)
        if len(buffer) == chunk_size:
            yield buffer
            buffer = []
    if buffer:
        yield buffer
