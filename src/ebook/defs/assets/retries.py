from datetime import timedelta
from random import randint

import dagster as dg


class FlakyResource(dg.ConfigurableResource):
    def addition(self, a: int, b: int) -> int:
        if randint(0, 1) == 1:
            raise ValueError("Flaky error")
        return a + b


@dg.asset(
    deps=["customers"],
    group_name="analysis",
    tags={
        "team_size": "x-large",
        "owner": "analytics_engineering",
    },
)
def flaky_asset(context: dg.AssetExecutionContext, flaky: FlakyResource):
    try:
        flaky.addition(1, 2)
    except ValueError:
        raise dg.RetryRequested(seconds_to_wait=5)


hourly_flaky_asset_freshness_check = dg.build_last_update_freshness_checks(
    assets=[flaky_asset],
    lower_bound_delta=timedelta(minutes=5),
)

# Define freshness check sensor
freshness_checks_sensor = dg.build_sensor_for_freshness_checks(
    freshness_checks=hourly_flaky_asset_freshness_check
)
