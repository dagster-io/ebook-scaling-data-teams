from setuptools import find_packages, setup

setup(
    name="ebook",
    packages=find_packages(exclude=["tests"]),
    install_requires=[
        "dagster",
        "dagster-webserver",
        "dagster-dg-cli",
        "dagster-aws",
        "dagster-dbt",
        "dagster-duckdb",
        "dagster-dlt",
        "dagster-sling",
        "dbt-duckdb",
        "dlt[s3, parquet]",
        "pandas",
        "numpy",
    ],
    extras_require={"dev": ["ruff", "pytest"]},
)
