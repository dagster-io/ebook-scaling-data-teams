import dagster as dg
import pytest

from src.ebook.defs.assets.flaky import flaky_asset
from tests.fixtures import defs  # noqa: F401


class TestFlakyResource(dg.ConfigurableResource):
    def addition(self, a: int, b: int) -> int:
        return a + b


def test_flaky_asset():
    context = dg.build_asset_context()
    flaky_asset(context, TestFlakyResource())


@pytest.mark.integration
def test_20_person_team(defs):  # noqa: F811
    pass
