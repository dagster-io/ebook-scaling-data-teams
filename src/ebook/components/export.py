import io

import dagster as dg
from dagster_aws.s3 import S3Resource
from dagster_duckdb import DuckDBResource


class ExportModel(dg.Model):
    table: str
    s3_path: str


class Export(dg.Component, dg.Model, dg.Resolvable):
    s3_bucket: str
    s3_region: str
    duckdb_database: str
    export_steps: list[ExportModel]

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _upload_assets = []

        for export_config in self.export_steps:

            @dg.asset(
                name=f"{export_config.table}_s3_export",
                deps=[f"{export_config.table}"],
                kinds={"duckdb", "s3"},
            )
            def _upload_to_s3(
                context: dg.AssetExecutionContext,
                export_s3: S3Resource,
                export_database: DuckDBResource,
            ) -> dg.MaterializeResult:
                # Read data from DuckDB
                with export_database.get_connection() as conn:
                    df = conn.execute(f"""
                        SELECT * FROM {export_config.table}
                    """).fetch_df()

                # Convert DataFrame to CSV
                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False)
                csv_data = csv_buffer.getvalue()

                # Upload to S3
                s3_client = export_s3.get_client()
                s3_client.put_object(
                    Bucket=self.s3_bucket,
                    Key=export_config.s3_path,
                    Body=csv_data,
                    ContentType="text/csv",
                )

                return dg.MaterializeResult(
                    metadata={
                        "table_name": export_config.table,
                        "s3_path": f"s3://{self.s3_bucket}/{export_config.s3_path}",
                    }
                )

            _upload_assets.append(_upload_to_s3)

        external_s3 = dg.AssetSpec(
            "processed_data_bucket",
            deps=_upload_assets,
            kinds={"s3"},
        )

        s3_resource = S3Resource(
            bucket="ebook",
            region="us-east-1",
            aws_access_key_id="test",
            aws_secret_access_key="test",
            endpoint_url="http://localhost:4566",
        )

        duckdb_resource = DuckDBResource(database=self.duckdb_database)

        return dg.Definitions(
            assets=_upload_assets + [external_s3],
            resources={
                "export_s3": s3_resource,
                "export_database": duckdb_resource,
            },
        )
