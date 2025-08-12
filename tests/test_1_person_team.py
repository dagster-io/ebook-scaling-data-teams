from unittest.mock import Mock

import dagster as dg
import pandas as pd
import pytest

import ebook.defs
from ebook.defs.assets.analysis import customer_analysis
from ebook.defs.resources import duckdb_resource
from tests.fixtures import docker_compose  # noqa: F401


@pytest.fixture()
def defs():
    return dg.Definitions.merge(dg.components.load_defs(ebook.defs))


@pytest.mark.integration
def test_1_person_team(defs, docker_compose):  # noqa: F811
    result = dg.materialize(
        assets=[
            defs.get_assets_def(dg.AssetKey(["target", "data", "customers"])),
            defs.get_assets_def(dg.AssetKey(["target", "data", "orders"])),
            defs.get_assets_def(dg.AssetKey(["target", "data", "payments"])),
            defs.get_assets_def("stg_customers"),
            defs.get_assets_def("stg_orders"),
            defs.get_assets_def("stg_payments"),
            defs.get_assets_def("customers"),
            defs.get_assets_def("customer_analysis"),
        ],
        resources={
            "database": duckdb_resource,
        },
    )
    assert result.success


def test_customer_analysis():
    mock_response = Mock()
    context = dg.build_asset_context()

    mock_connection = Mock()
    mock_connection.execute.return_value.fetch_df.return_value = pd.DataFrame(
        {
            "customer_id": [1],
            "first_name": ["John"],
            "last_name": ["Doe"],
            "number_of_orders": [10],
        }
    )

    mock_response.get_connection.return_value.__enter__ = Mock(
        return_value=mock_connection
    )
    mock_response.get_connection.return_value.__exit__ = Mock(return_value=None)

    result = customer_analysis(context, mock_response)
    assert result.metadata["customer_name"] == "John Doe"
    assert result.metadata["number_of_orders"] == 10
