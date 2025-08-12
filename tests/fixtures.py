import os
import subprocess
import time
from pathlib import Path

import pytest


@pytest.fixture(scope="session", autouse=True)
def docker_compose():
    # Start Docker Compose
    file_path = Path(__file__).absolute().parent.parent / "docker-compose.yaml"
    subprocess.run(
        ["docker", "compose", "-f", file_path, "up", "--build", "-d"],
        check=True,
        capture_output=True,
    )

    max_retries = 5
    for i in range(max_retries):
        result = subprocess.run(
            ["docker", "exec", "postgresql", "pg_isready"],
            capture_output=True,
        )
        if result.returncode == 0:
            break
        time.sleep(5)

    yield

    # Tear down Docker Compose
    subprocess.run(["docker", "compose", "-f", file_path, "down"], check=True)


@pytest.fixture(scope="session", autouse=True)
def dbt_project():
    dir = os.path.join(
        os.path.dirname(__file__),
        "../dbt_project",
    )

    cmd = ["dbt", "parse", "--project-dir", dir, "--profiles-dir", dir]
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        pytest.fail(f"dbt command failed: {result.returncode}")
