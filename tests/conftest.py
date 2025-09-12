import pytest


@pytest.fixture(autouse=True)
def duckdb_path_env(monkeypatch):
    monkeypatch.setenv("DUCKDB_DATABASE", "/tmp/ebook.duckdb")
