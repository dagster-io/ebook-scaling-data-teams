import dagster as dg
from dagster_duckdb import DuckDBResource


@dg.asset(
    deps=["customers"],
    group_name="analysis",
    tags={
        "team_size": "small",
        "owner": "analytics_engineering",
    },
    kinds={"duckdb"},
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

    # Check within asset, better as an asset check
    if df["number_of_orders"].sum() <= 0:
        raise ValueError("Number of orders must be greater than zero")

    return dg.MaterializeResult(
        metadata={
            "customer_name": f"{df.iloc[0]['first_name']} {df.iloc[0]['last_name']}",
            "number_of_orders": int(df.iloc[0]["number_of_orders"]),
        }
    )


@dg.asset_check(asset="customer_analysis")
def customer_analysis_check(
    context: dg.AssetExecutionContext, database: DuckDBResource
) -> dg.AssetCheckResult:
    with database.get_connection() as conn:
        df = conn.execute("""
            SELECT 
                customer_id,
                first_name,
                last_name,
                number_of_orders
            FROM customers
        """).fetch_df()

    if df["number_of_orders"].sum() <= 0:
        return dg.AssetCheckResult(
            success=False,
            description="Number of orders must be greater than zero",
        )

    return dg.AssetCheckResult(success=True)


@dg.asset(
    deps=["localstack_s3_source_stock_1_data"],
    group_name="analysis",
    tags={
        "team_size": "medium",
        "owner": "analytics_engineering",
    },
    kinds={"s3"},
)
def s3_analysis(context: dg.AssetExecutionContext) -> dg.MaterializeResult: ...
