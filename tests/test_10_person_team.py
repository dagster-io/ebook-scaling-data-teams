import dagster as dg
import pytest
from dagster.components.testing.utils import create_defs_folder_sandbox
from dagster_aws.s3 import S3Resource
from dagster_duckdb import DuckDBResource

from ebook.components.export import Export
from ebook.defs.resources import duckdb_resource
from tests.fixtures import dbt_project, defs, docker_compose  # noqa: F401

yaml_config = {
    "type": "ebook.components.export.Export",
    "attributes": {
        "s3_bucket": "ebook",
        "s3_region": "us-east-1",
        "duckdb_database": "/tmp/ebook.duckdb",
        "export_steps": [
            {"table": "stg_payments", "s3_path": "payments.csv"},
        ],
    },
}


def test_scaffold_export():
    with create_defs_folder_sandbox() as sandbox:
        defs_path = sandbox.scaffold_component(component_cls=Export)
        assert (defs_path / "defs.yaml").exists()


@pytest.mark.integration
def test_export_component(defs, docker_compose, dbt_project):  # noqa: F811
    with create_defs_folder_sandbox() as sandbox:
        defs_path = sandbox.scaffold_component(component_cls=Export)
        sandbox.scaffold_component(
            component_cls=Export, defs_path=defs_path, defs_yaml_contents=yaml_config
        )

        # Check that all assets are created
        with sandbox.build_all_defs() as sandbox_defs:
            assert sandbox_defs.resolve_asset_graph().get_all_asset_keys() == {
                dg.AssetKey(["stg_payments"]),
                dg.AssetKey(["stg_payments_s3_export"]),
                dg.AssetKey(["processed_data_bucket"]),
            }

            # Ensure that the assets execute correctly
            result = dg.materialize(
                assets=[
                    defs.get_assets_def(dg.AssetKey(["target", "data", "payments"])),
                    defs.get_assets_def("stg_payments"),
                    sandbox_defs.get_assets_def(
                        dg.AssetKey(["stg_payments_s3_export"])
                    ),
                ],
                # TODO: Need to set resources explicitly though the component generates these
                resources={
                    "export_database": DuckDBResource(database="/tmp/ebook.duckdb"),
                    "export_s3": S3Resource(
                        bucket="ebook",
                        region="us-east-1",
                        aws_access_key_id="test",
                        aws_secret_access_key="test",
                        endpoint_url="http://localhost:4566",
                    ),
                },
            )
            assert result.success


@pytest.mark.integration
def test_10_person_team(defs, docker_compose, dbt_project):  # noqa: F811
    result = dg.materialize(
        assets=[
            # Full stock ingestion and analysis
            defs.get_assets_def(dg.AssetKey(["localstack_data", "stock_1_data"])),
            defs.get_assets_def(dg.AssetKey(["localstack_data", "stock_2_data"])),
            defs.get_assets_def("stocks"),
            defs.get_assets_def("stock_analysis"),
        ],
        resources={
            "database": duckdb_resource,
        },
    )
    assert result.success
