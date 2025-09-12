#!/bin/bash

# Start Docker Compose services
docker compose up -d

# Start Dagster web server
dg dev