import dagster as dg
from dagster_duckdb import DuckDBResource

from ebook.defs.assets.flaky import FlakyResource

flaky_resource = FlakyResource()

duckdb_resource = DuckDBResource(database="/tmp/ebook.duckdb")


@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "database": duckdb_resource,
            "flaky": flaky_resource,
        },
    )
