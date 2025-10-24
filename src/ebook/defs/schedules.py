from typing import Union

import dagster as dg


@dg.schedule(
    cron_schedule="0 */6 * * *",
    target=[
        dg.AssetKey(["target", "data", "customers"]),
        dg.AssetKey(["target", "data", "orders"]),
        dg.AssetKey(["target", "data", "payments"]),
    ],
)
def sling_schedule(
    context: dg.ScheduleEvaluationContext,
) -> Union[dg.RunRequest, dg.SkipReason]:
    return dg.SkipReason(
        "Skipping. Change this to return a RunRequest to launch a run."
    )
