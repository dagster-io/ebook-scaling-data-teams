import dagster as dg
import pytest

from ebook.defs.resources import duckdb_resource
from tests.fixtures import defs  # noqa: F401


@pytest.mark.integration
def test_10_person_team(defs):  # noqa: F811
    result = dg.materialize(
        assets=[
            defs.get_assets_def("customers_extract"),
            # defs.get_assets_def("customers_s3_export"),
            defs.get_assets_def("stg_payments_extract"),
            # defs.get_assets_def("stg_payments_s3_export"),
        ],
        resources={
            "database": duckdb_resource,
        },
    )
    assert result.success
