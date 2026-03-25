# World Bank Economic Indicators ETL Pipeline

git clone https://github.com/likhitgh/WorldBank_Assessment_Confluent/new/master?filename=README.md

## Overview
This repository contains an end-to-end Data Engineering pipeline that extracts global economic and financial indicator data from the World Bank API, stages it, transforms it into a structured Data Warehouse, and exposes it via a denormalized reporting layer. 

The pipeline specifically ingests GDP per capita, Inflation, and Unemployment data alongside a Country dimension, modeling it into a Star Schema optimized for downstream analytics.

## Prerequisites
* Docker 

## Quickstart: Running Locally
This solution is fully containerized to ensure it can be stood up with minimal friction. To launch the Postgres database and Apache Airflow orchestrator, run:

docker-compose up --build

Once the containers are healthy:

Navigate to http://localhost:8080 in your browser.

Log into the Airflow UI (Username: airflow / Password: airflow).

Locate the worldbank_economic_pipeline DAG and trigger it manually to begin the run.

Running Tests
Unit tests have been written using mocks to ensure the extraction and transformation logic can be validated without requiring a live database or external API connections. To run the test suite locally:

pytest

Architecture & Design Decisions
1. Tool Choices & Avoiding Over-Engineering
While distributed processing frameworks like Apache Spark or Databricks are standard for large-scale enterprise data processing, the World Bank dataset volume does not justify the overhead of a distributed cluster. To adhere to the principle of "not over-engineering," I selected a lightweight, highly effective ELT stack:
Python (Requests/Psycopg2): Chosen for memory-efficient API extraction and pagination handling.
PostgreSQL: Serves as both the staging area (leveraging native JSONB support for raw data) and the structured Data Warehouse.
Apache Airflow: Provides robust orchestration, dependency management, and scheduling via the LocalExecutor.

2. Separation of Concerns (The ELT Pattern)
The pipeline is strictly separated into distinct phases:
Extract & Load (extract.py): Reaches out to the paginated REST APIs and lands the raw, unaltered JSON payload directly into a staging schema. This decoupling ensures that if downstream transformations fail, we do not need to re-query the external API.
Transform (transform.py): Executes SQL within the database to parse the JSONB, enforce data typing, and model the relational schemas.

3. Data Modeling & Schema Design
The data is modeled into a Star Schema within the warehouse schema.
Dimensions (dim_country, dim_indicator): Holds descriptive attributes. The country hierarchy (Region and Income Level) is retained directly within dim_country to avoid a snowflake design, reducing JOIN complexity for consumers.
Fact (fact_economic_indicators): Highly normalized, containing the numeric indicator values.
Idempotency: The pipeline uses ON CONFLICT DO NOTHING logic on primary and composite keys (country_code, indicator_code, year) to ensure safe, duplicate-free re-runs.

4. Handling Referential Integrity
In real-world pipelines, dimensions often lag behind facts. If a fact record references a country code that does not yet exist in dim_country, the pipeline does not silently drop the record or fail the load. Instead, the missing reference is intercepted and logged into an audit_missing_countries table. This provides a clear audit trail for data stewards to investigate without breaking the automated ETL flow.

5. The Reporting Layer
Analysts often require flat structures for BI tools (like Tableau or PowerBI) and cannot always write complex SQL. A denormalized view (warehouse.rpt_economic_indicators) is generated at the end of the pipeline. It pivots the data, transforming rows of indicators into distinct columns (gdp_per_capita, inflation_rate, unemployment_rate) grouped by country and year so analysts can query the data directly without JOINS.

Known Limitations & Future Improvements
Given more time, I would implement the following enhancements:

Data Quality Checks (Data Contracts): Integrate a framework like Great Expectations between the staging and warehouse layers to validate numerical thresholds (e.g., ensuring percentages fall within expected bounds) and schema enforcement.

Dynamic Incremental Loading: Currently, the date range for extraction is scoped to a static range. I would upgrade the Airflow DAG to dynamically pass execution dates (ds) to the API parameters, storing watermarks to truly support incremental daily loads.

Alerting: I would have configured  Airflow Slack/Email operators to alert the team on task failures.
