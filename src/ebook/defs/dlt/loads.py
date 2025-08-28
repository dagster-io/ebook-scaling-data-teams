from typing import Iterator

import dlt
import pandas as pd


@dlt.source
def localstack_s3_source():
    @dlt.resource
    def stock_1_data() -> Iterator[pd.DataFrame]:
        """Load stock_1.csv from localstack S3 bucket"""
        endpoint_url = "http://localhost:4566"
        bucket_name = "ebook"
        file_key = "stock_1.csv"

        s3_path = f"s3://{bucket_name}/{file_key}"
        df = pd.read_csv(
            s3_path,
            storage_options={
                "endpoint_url": endpoint_url,
                "key": "test",
                "secret": "test",
            },
        )

        yield df

    @dlt.resource
    def stock_2_data() -> Iterator[pd.DataFrame]:
        """Load stock_2.csv from localstack S3 bucket"""
        endpoint_url = "http://localhost:4566"
        bucket_name = "ebook"
        file_key = "stock_2.csv"

        s3_path = f"s3://{bucket_name}/{file_key}"
        df = pd.read_csv(
            s3_path,
            storage_options={
                "endpoint_url": endpoint_url,
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
    destination="duckdb",
    dataset_name="localstack_data",
)
