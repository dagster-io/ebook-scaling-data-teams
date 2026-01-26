FROM mcr.microsoft.com/devcontainers/python:3.12-bookworm
ENV PYTHONUNBUFFERED 1

COPY --from=ghcr.io/astral-sh/uv:0.6.10 /uv /bin/uv

# Copy the entire project to install the local package
COPY . /workspace
WORKDIR /workspace

# Install the project in editable mode with all dependencies
RUN uv pip install -e . --system