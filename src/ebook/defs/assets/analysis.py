import dagster as dg


@dg.asset(
    deps=["customers"],
    group_name="analysis",
    tags={
        "team_size": "small",
        "owner": "analytics_engineering",
    },
)
def customer_analysis(context: dg.AssetExecutionContext) -> dg.MaterializeResult: ...


@dg.asset(
    deps=["s3"],
    group_name="analysis",
    tags={
        "team_size": "medium",
        "owner": "analytics_engineering",
    },
)
def s3_analysis(context: dg.AssetExecutionContext) -> dg.MaterializeResult: ...
