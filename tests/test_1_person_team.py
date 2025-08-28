import dagster as dg
import pytest

from ebook.defs.resources import duckdb_resource
from tests.fixtures import dbt_project, defs, docker_compose  # noqa: F401


@pytest.mark.integration
def test_1_person_team(defs, docker_compose, dbt_project):  # noqa: F811
    result = dg.materialize(
        assets=[
            defs.get_assets_def(dg.AssetKey(["target", "data", "customers"])),
            defs.get_assets_def(dg.AssetKey(["target", "data", "orders"])),
            defs.get_assets_def(dg.AssetKey(["target", "data", "payments"])),
            defs.get_assets_def("stg_customers"),
            defs.get_assets_def("stg_orders"),
            defs.get_assets_def("stg_payments"),
            defs.get_assets_def("customers"),
        ],
        resources={
            "database": duckdb_resource,
        },
    )
    assert result.success
