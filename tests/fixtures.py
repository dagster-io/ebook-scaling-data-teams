import os
import subprocess
import time
from pathlib import Path

import dagster as dg
import pytest

import ebook.defs


@pytest.fixture()
def defs():
    return dg.components.load_defs(ebook.defs)


@pytest.fixture(scope="session")
def docker_compose():
    # Start Docker Compose
    file_path = Path(__file__).absolute().parent.parent / "docker-compose.yaml"
    subprocess.run(
        ["docker", "compose", "-f", file_path, "up", "--build", "-d"],
        check=True,
        capture_output=True,
    )

    # Wait for PostgreSQL to be ready
    max_retries = 12  # Increased retries for CI
    for i in range(max_retries):
        result = subprocess.run(
            ["docker", "exec", "postgresql", "pg_isready", "-U", "test_user", "-d", "test_db"],
            capture_output=True,
        )
        if result.returncode == 0:
            break
        time.sleep(5)
    else:
        # If we get here, PostgreSQL never became ready
        subprocess.run(["docker", "compose", "-f", file_path, "logs", "postgresql"], check=False)
        raise RuntimeError("PostgreSQL failed to start within timeout")

    # Wait for LocalStack to be ready
    max_retries = 12
    for i in range(max_retries):
        result = subprocess.run(
            ["curl", "-f", "http://localhost:4566/_localstack/health"],
            capture_output=True,
        )
        if result.returncode == 0:
            break
        time.sleep(5)
    else:
        # If we get here, LocalStack never became ready
        subprocess.run(["docker", "compose", "-f", file_path, "logs", "localstack"], check=False)
        raise RuntimeError("LocalStack failed to start within timeout")

    yield

    # Tear down Docker Compose
    subprocess.run(["docker", "compose", "-f", file_path, "down"], check=True)


@pytest.fixture(scope="session")
def dbt_project():
    dir = os.path.join(
        os.path.dirname(__file__),
        "../dbt_project",
    )

    cmd = ["dbt", "parse", "--project-dir", dir, "--profiles-dir", dir]
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        pytest.fail(f"dbt command failed: {result.returncode}")
