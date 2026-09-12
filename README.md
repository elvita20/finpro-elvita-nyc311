# NYC 311 Data Engineering Final Project

Final Project JCDEAH-009

## Dataset

NYC 311 Service Requests from 2020 to Present

Dataset ID:

erm2-nwe9

## Project Objective

Build an end-to-end data engineering pipeline to ingest,
process, transform, validate, and visualize NYC 311
service request data.

## Architecture

NYC Open Data
→ Apache Airflow
→ Google Cloud Storage
→ BigQuery Staging
→ BigQuery Data Warehouse
→ dbt
→ Analytics Mart
→ Looker Studio

## Technologies

- Python
- Apache Airflow
- Docker
- Docker Compose
- Google Cloud Platform
- Google Cloud Storage
- BigQuery
- dbt
- Pub/Sub
- Looker Studio

## Pipeline

### Batch

NYC Socrata API
→ Airflow
→ GCS
→ BigQuery Staging
→ BigQuery Warehouse

### Streaming

Will be implemented using event replay:

NYC 311 events
→ Pub/Sub
→ Stream Processing
→ BigQuery