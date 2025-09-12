import dagster as dg
import numpy as np
import pandas as pd
from dagster_duckdb import DuckDBResource


@dg.asset(
    deps=["customers"],
    group_name="analysis",
    tags={
        "team_size": "medium",
        "owner": "analytics_engineering",
    },
    kinds={"duckdb", "pandas"},
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

    # Check within asset, better as an asset check (see below)
    if df["number_of_orders"].sum() <= 0:
        raise ValueError("Number of orders must be greater than zero")

    return dg.MaterializeResult(
        metadata={
            "customer_name": f"{df.iloc[0]['first_name']} {df.iloc[0]['last_name']}",
            "number_of_orders": int(df.iloc[0]["number_of_orders"]),
        }
    )


@dg.asset_check(asset="customer_analysis")
def customer_analysis_check(database: DuckDBResource) -> dg.AssetCheckResult:
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
            passed=False,
            description="Number of orders must be greater than zero",
        )

    return dg.AssetCheckResult(passed=True)


@dg.asset(
    deps=["stocks"],
    group_name="analysis",
    tags={
        "team_size": "large",
        "owner": "analytics_engineering",
    },
    kinds={"duckdb", "pandas"},
)
def stock_analysis(
    context: dg.AssetExecutionContext, database: DuckDBResource
) -> dg.MaterializeResult:
    with database.get_connection() as conn:
        df = conn.execute("""
            SELECT date, open, close, high, low, volume 
            FROM stocks 
            ORDER BY date
        """).fetch_df()

    # Convert date column to datetime
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    # Calculate daily returns
    df["daily_return"] = df["close"].pct_change()

    # Calculate volatility (rolling standard deviation)
    df["volatility_10"] = df["daily_return"].rolling(window=10).std()

    return dg.MaterializeResult(
        metadata={
            "volatility_10": f"{df['volatility_10']}%",
        }
    )
