# Scaling Data Teams

A comprehensive data engineering platform demonstrating best practices and patterns for teams of different sizes. This project showcases how data engineering practices evolve from individual contributors to large-scale engineering organizations.

## 🏗️ Architecture Overview

This project implements a modern data stack using:

- **[Dagster](https://dagster.io/)** - Data orchestration and asset management
- **[dbt](https://www.getdbt.com/)** - Data transformation and modeling
- **[dlt](https://dlthub.com/)** - Data loading and ingestion
- **[Sling](https://slingdata.io/)** - Data replication and streaming
- **[DuckDB](https://duckdb.org/)** - Analytics database

### Supporting Infrastructure

- **PostgreSQL** - Relational database for structured data
- **LocalStack** - Local AWS S3 emulation

## 📊 Team Size Patterns

This project demonstrates data engineering patterns for different organizational scales:

- **1 Person Team** - Individual contributor patterns with basic dbt transformations
- **5 Person Team** - Small team collaboration with asset dependencies
- **10 Person Team** - Medium team structure with specialized roles
- **20 Person Team** - Large team patterns with advanced orchestration

### Start Infrastructure Services

```bash
docker-compose up -d
```

### Run Dagster Web Server

```bash
dg dev
```

The Dagster UI will be available at [http://localhost:3000](http://localhost:3000)

## 📁 Project Structure

```
├── src/ebook/                    # Main Dagster Python package
│   ├── components/               # Reusable Dagster components
│   │   └── export.py             # Custom Dagster S3 export component
│   ├── defs/                     # Dagster definitions
│   │   ├── assets/               # Data assets
│   │   ├── dbt/                  # dbt integration
│   │   ├── dlt/                  # dlt integration
│   │   ├── export/               # Custom Dagster S3 component configuration
│   │   └── sling/                # Sling integration
│   └── definitions.py            # Main definitions entry point
├── dbt_project/                  # dbt transformations
├── tests/                        # Test scenarios by team size
└── docker-compose.yaml           # Infrastructure services
```