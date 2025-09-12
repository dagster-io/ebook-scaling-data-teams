from typing import Iterator

import dlt
import pandas as pd

HEADERS = ["date", "close", "volume", "open", "high", "low"]
ENDPOINT_URL = "http://localhost:4566"
BUCKET_NAME = "ebook"


@dlt.source
def localstack_s3_source():
    @dlt.resource
    def stock_1_data() -> Iterator[pd.DataFrame]:
        """Load stock_1.csv from localstack S3 bucket"""
        file_key = "stock_1.csv"

        s3_path = f"s3://{BUCKET_NAME}/{file_key}"
        df = pd.read_csv(
            s3_path,
            header=None,
            names=HEADERS,
            storage_options={
                "endpoint_url": ENDPOINT_URL,
                "key": "test",
                "secret": "test",
            },
        )

        yield df

    @dlt.resource
    def stock_2_data() -> Iterator[pd.DataFrame]:
        """Load stock_2.csv from localstack S3 bucket"""
        file_key = "stock_2.csv"

        s3_path = f"s3://{BUCKET_NAME}/{file_key}"
        df = pd.read_csv(
            s3_path,
            header=None,
            names=HEADERS,
            storage_options={
                "endpoint_url": ENDPOINT_URL,
                "key": "test",
                "secret": "test",
            },
        )

        yield df

    return stock_1_data, stock_2_data


# Create the source and pipeline
localstack_source = localstack_s3_source()
localstack_pipeline = dlt.pipeline(
    pipeline_name="localstack_loader",
    destination=dlt.destinations.duckdb("/tmp/ebook.duckdb"),
    dataset_name="localstack_data",
)
