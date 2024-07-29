import json
import os
import tempfile
import pytest as pytest
import pandas as pd

from src.ingestion.ingestion import get_items, APIError, save_to_parquet
import responses
from urllib.parse import urlparse, parse_qs


@responses.activate
def test_get_items_no_items():
    endpoint = "http://127.0.0.1:8000/some-model"
    page_size = 10

    responses.add(
        responses.GET,
        endpoint,
        json={
            "items": [],
            "page": 1,
            "pages": 1,
            "size": page_size,
            "total": 0,
        },
        status=200,
    )

    data = list(get_items(endpoint, page_size))
    assert len(data) == 0
    assert data == []


@responses.activate
def test_get_items_single_page():
    endpoint = "http://127.0.0.1:8000/some-model"
    page_size = 10

    responses.add(
        responses.GET,
        endpoint,
        json={
            "items": [1, 2, 3],
            "page": 1,
            "pages": 1,
            "size": page_size,
            "total": 3,
        },
        status=200,
    )

    data = list(get_items(endpoint, page_size))
    assert len(data) == 3
    assert data == [1, 2, 3]


# TODO test 404
# TODO test retries


@responses.activate
def test_get_items_multiple_pages():
    endpoint = "http://127.0.0.1:8000/some-model"
    page_size = 2

    def request_callback(request):
        query_params = parse_qs(urlparse(request.url).query)
        page = query_params.get("page", [None])[0]

        if page == "1":
            return (
                200,
                {},
                json.dumps(
                    {
                        "items": [1, 2],
                        "page": 1,
                        "pages": 2,
                        "size": page_size,
                        "total": 3,
                    }
                ),
            )
        elif page == "2":
            return (
                200,
                {},
                json.dumps(
                    {
                        "items": [3],
                        "page": 2,
                        "pages": 2,
                        "size": page_size,
                        "total": 3,
                    }
                ),
            )
        else:
            return (
                404,
                {},
                json.dumps(
                    {
                        "error": "Not found",
                    }
                ),
            )

    responses.add_callback(
        responses.GET,
        endpoint,
        callback=request_callback,
        content_type="application/json",
    )

    data = list(get_items(endpoint, page_size))
    assert len(data) == 3
    assert data == [1, 2, 3]


@responses.activate
def test_get_items_error_is_propagated():
    endpoint = "http://127.0.0.1:8000/some-model"
    page_size = 10

    responses.add(responses.GET, endpoint, json={}, status=404)

    with pytest.raises(APIError):
        list(get_items(endpoint, page_size))


def test_save_to_parquet():
    data = [
        {"id": 1, "name": "Alice", "age": 30},
        {"id": 2, "name": "Bob", "age": 25},
        {"id": 3, "name": "Charlie", "age": 35},
    ]

    with tempfile.TemporaryDirectory() as temp_dir:
        output_file = os.path.join(temp_dir, "test_output.parquet")
        save_to_parquet(data, output_file, buffer_size=1000)
        assert os.path.exists(output_file)
        df_read = pd.read_parquet(output_file)
        df_expected = pd.DataFrame(data)
        pd.testing.assert_frame_equal(df_read, df_expected)


def test_save_to_parquet_in_chunks():
    data = [
        {"id": 1, "name": "Alice", "age": 30},
        {"id": 2, "name": "Bob", "age": 25},
        {"id": 3, "name": "Charlie", "age": 35},
        {"id": 4, "name": "Charlie2", "age": 44},
        {"id": 5, "name": "Charlie3", "age": 101},
    ]

    with tempfile.TemporaryDirectory() as temp_dir:
        output_file = os.path.join(temp_dir, "test_output.parquet")
        save_to_parquet(data, output_file, buffer_size=2)
        assert os.path.exists(output_file)
        df_read = pd.read_parquet(output_file)
        df_expected = pd.DataFrame(data)
        pd.testing.assert_frame_equal(df_read, df_expected)
