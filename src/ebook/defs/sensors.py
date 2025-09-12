from datetime import datetime, timedelta

import dagster as dg
from dagster_duckdb import DuckDBResource


@dg.sensor(
    name="new_customers_sensor",
    minimum_interval_seconds=30,
)
def new_customers_sensor(
    context: dg.SensorEvaluationContext, duckdb: DuckDBResource
) -> dg.SensorEvaluationContext:
    """Sensor that monitors the customers table for new customers added in the last hour."""

    # Get the last check time from sensor context
    last_check = context.cursor or (datetime.now() - timedelta(hours=1)).isoformat()

    # Query for new customers since last check
    with duckdb.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT customer_id, first_name, last_name, email
            FROM data.customers 
            WHERE customer_id > (
                SELECT COALESCE(MAX(customer_id), 0) 
                FROM data.customers 
                WHERE customer_id <= %s
            )
            ORDER BY customer_id
            """,
            (int(last_check) if last_check.isdigit() else 0,),
        )

        new_customers = cursor.fetchall()

    if new_customers:
        context.log.info(f"Found {len(new_customers)} new customers")

        # Log details of new customers
        for customer in new_customers:
            customer_id, first_name, last_name, email = customer
            context.log.info(
                f"New customer: {customer_id} - {first_name} {last_name} ({email})"
            )

        # Update cursor to the highest customer_id found
        max_customer_id = max(customer[0] for customer in new_customers)
        context.update_cursor(str(max_customer_id))

        # Return a run request to trigger downstream assets
        return dg.SensorEvaluationContext(
            run_requests=[
                dg.RunRequest(
                    run_key=f"new_customers_{max_customer_id}_{int(datetime.now().timestamp())}",
                    tags={
                        "trigger": "new_customers_sensor",
                        "customer_count": str(len(new_customers)),
                    },
                )
            ]
        )
    else:
        context.log.info("No new customers found")
        return dg.SensorEvaluationContext()
