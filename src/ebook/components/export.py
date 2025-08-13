import dagster as dg
import pandas as pd
from dagster_aws.s3 import S3Resource


class Export(dg.Model):
    table: str
    s3_path: str


class Export(dg.Component, dg.Model, dg.Resolvable):
    s3_bucket: str
    s3_region: str
    export_steps: list[Export]

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _export_assets = []

        for export in self.export_steps:

            @dg.asset(
                name=f"{export.table}_extract",
                deps=[export.table],
                kinds={"pandas", "component"},
                tags={"team_size": "large", "owner": "data_engineering"},
            )
            def _table():
                df = pd.DataFrame({"column1": [1, 2, 3], "column2": ["A", "B", "C"]})

                return df.to_csv(index=False)

            @dg.asset(
                name=f"{export.table}_s3_export",
                deps=[f"{export.table}_extract"],
                kinds={"s3"},
                tags={"team_size": "large", "owner": "data_engineering"},
            )
            def _upload_to_s3(export: S3Resource):
                s3_client = export.get_client()
                s3_path = "export.s3_path"

                # csv_data = "dddd"

                # s3_client.put_object(
                #     Bucket=self.s3_bucket,
                #     Key=s3_path,
                #     Body=csv_data,
                # )

            _export_assets.extend([_table, _upload_to_s3])

        s3_resource = S3Resource(
            bucket="ebook",
            region="us-east-1",
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )

        return dg.Definitions(
            assets=_export_assets,
            resources={"export": s3_resource},
        )
