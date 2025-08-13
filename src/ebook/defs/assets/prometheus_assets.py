import dagster as dg
from dagster_prometheus import PrometheusResource


@dg.asset(
    group_name="prometheus",
    tags={
        "owner": "data_engineering",
    },
)
def prometheus_metric(prometheus: PrometheusResource):
    prometheus.push_to_gateway(job="my_job_label")
