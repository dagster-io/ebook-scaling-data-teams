import dagster as dg
from dagster_duckdb import DuckDBResource


@dg.asset(
    deps=[
        dg.AssetKey(["localstack_data", "stock_1_data"]),
        dg.AssetKey(["localstack_data", "stock_2_data"]),
    ],
    group_name="transform",
    tags={
        "team_size": "medium",
        "owner": "analytics_engineering",
    },
    kinds={"duckdb"},
)
def stocks(context: dg.AssetExecutionContext, database: DuckDBResource) -> None:
    query = """
        CREATE TABLE IF NOT EXISTS stocks AS (
            SELECT * FROM localstack_data.stock_1_data
            UNION ALL
            SELECT * FROM localstack_data.stock_2_data
        )
    """

    with database.get_connection() as conn:
        conn.execute(query)
