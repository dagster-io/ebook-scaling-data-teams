import dagster as dg
from dagster_aws.s3 import S3Resource
from dagster_duckdb import DuckDBResource
from dagster_prometheus import PrometheusResource

duckdb_resource = DuckDBResource(
    database=dg.EnvVar("DUCKDB_DATABASE"),
)

prometheus_resource = PrometheusResource(
    gateway="http://localhost:80",
)

s3_resource = S3Resource(
    bucket="ebook",
    region="us-east-1",
    aws_access_key_id="test",
    aws_secret_access_key="test",
)


@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "database": duckdb_resource,
            "prometheus": prometheus_resource,
            "import": s3_resource,
        },
    )
