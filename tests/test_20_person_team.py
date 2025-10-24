import dagster as dg

from src.ebook.defs.assets.retries import flaky_asset
from tests.fixtures import defs  # noqa: F401


class TestFlakyResource(dg.ConfigurableResource):
    def addition(self, a: int, b: int) -> int:
        return a + b


def test_flaky_asset():
    context = dg.build_asset_context()
    flaky_asset(context, TestFlakyResource())
