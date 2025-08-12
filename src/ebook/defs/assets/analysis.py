import dagster as dg
from dagster_duckdb import DuckDBResource


@dg.asset(
    deps=["customers"],
    group_name="analysis",
    tags={
        "team_size": "small",
        "owner": "analytics_engineering",
    },
)
def customer_analysis(
    context: dg.AssetExecutionContext, database: DuckDBResource
) -> dg.MaterializeResult:
    with database.get_connection() as conn:
        df = conn.execute("""
            SELECT 
                customer_id,
                first_name,
                last_name,
                number_of_orders
            FROM customers
        """).fetch_df()

    return dg.MaterializeResult(
        metadata={
            "customer_name": f"{df.iloc[0]['first_name']} {df.iloc[0]['last_name']}",
            "number_of_orders": int(df.iloc[0]["number_of_orders"]),
        }
    )


@dg.asset(
    deps=["s3"],
    group_name="analysis",
    tags={
        "team_size": "medium",
        "owner": "analytics_engineering",
    },
)
def s3_analysis(context: dg.AssetExecutionContext) -> dg.MaterializeResult: ...
