import dagster as dg
import pytest

import ebook.defs
from tests.fixtures import docker_compose  # noqa: F401


@pytest.fixture()
def defs():
    return dg.Definitions.merge(dg.components.load_defs(ebook.defs))


@pytest.mark.integration
def test_postgres_component_sling_assets(defs, docker_compose):  # noqa: F811
    result = dg.materialize(
        assets=[
            defs.get_assets_def(dg.AssetKey(["target", "data", "customers"])),
            defs.get_assets_def(dg.AssetKey(["target", "data", "orders"])),
            defs.get_assets_def(dg.AssetKey(["target", "data", "payments"])),
        ],
    )
    assert result.success
