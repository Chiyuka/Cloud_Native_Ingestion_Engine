
WeatherFlow ETL - Cloud-Native Ingestion Engine

WeatherFlow is a robust, cloud-native ETL pipeline designed to ingest, validate, and aggregate weather data. It is built with a "fail loud, fail early" design principle to ensure that bad data never reaches the database without strict validation.

Architecture
The project consists of three main containerized services:

  Postgres:
    The data warehouse where clean raw data and hourly aggregates are stored.

  Ingestion:
    A Python service that fetches weather data from APIs and stores it as raw JSON.

  Spark:
    A PySpark batch processor that runs strict data quality checks, enforces schemas,
    and transforms the raw data before loading it into PostgreSQL.

Features

  Strict Schema Enforcement:
    Validates structure and data types before processing, with fallback mechanisms
    for missing optional columns.

  Data Quality Checks:
    Includes value range validation (e.g., temperature bounds, humidity percentages)
    and deduplication.

  Quarantine System:
    Identifies bad records and routes them to a dead-letter quarantine folder without
    silently swallowing errors.

  Pre-aggregated Data:
    Automatically builds hourly aggregations for fast dashboard querying.

  Audit Logging:
    Tracks pipeline execution metadata (success, failures, rows ingested, quarantined rows)
    in a dedicated PostgreSQL table.

Prerequisites

  - Docker
  - Docker Compose
  - OpenWeather API Key (or equivalent source API key)

Setup & Execution

  Environment Variables:
    Create a .env file in the root directory based on your specific credentials:

      POSTGRES_USER=etl_user
      POSTGRES_PASSWORD=etl_pass
      POSTGRES_DB=weatherflow
      OPENWEATHER_API_KEY=your_api_key_here

  Start the Infrastructure:
    Run the following command to spin up the PostgreSQL database and create the expected schemas:

      docker-compose up -d postgres

  Start the ETL Pipeline:
    Once the database is healthy, start the ingestion and spark transformation services:

      docker-compose up ingestion spark

Directory Structure

  /ingestion/
    Python service responsible for fetching raw API data.

  /spark/
    PySpark application handling data validation, quarantine logic, and transformations.

  /sql/
    Initialization scripts defining the database schema, constraints, and views.

  docker-compose.yml
    Container orchestration.
