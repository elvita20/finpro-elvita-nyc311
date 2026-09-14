# FinPro Elvita — NYC 311 Service Request Pipeline

Final Project JCDEAH-009 — end-to-end data pipeline untuk dataset **NYC 311 Service Requests**, dari ekstraksi data mentah sampai dashboard monitoring.

## Problem Statement & Business Goal

NYC 311 Service Request Monitoring — membantu pihak kota memahami jenis komplain warga yang paling sering muncul, dan mengukur kecepatan tiap agency dalam menyelesaikan komplain, sebagai dasar evaluasi kinerja dan alokasi sumber daya.

## Dashboard

🔗 [Looker Studio Dashboard](https://datastudio.google.com/s/iu5AbxR0lhE)

## Arsitektur

### Batch Pipeline (Airflow, harian)

```
NYC Open Data API (Socrata, dataset erm2-nwe9)
        |
        v
extract_to_gcs              Python: Socrata API -> GCS Data Lake
        |
        v
Google Cloud Storage         jcdeah-009-nyc311-datalake-elvita
        |
        v
load_gcs_to_staging          GCSToBigQueryOperator
        |
        v
BigQuery Staging              finpro_elvita_nyc311_staging.stg_311_daily
        |
        v
validate_staging              Data quality check
        |
        v
ensure_warehouse_table         DDL idempotent
        |
        v
merge_to_warehouse             MERGE / UPSERT
        |
        v
BigQuery Warehouse              finpro_elvita_nyc311_dw.fact_service_requests
        |
        v
validate_warehouse              Data quality check
        |
        v
vw_dashboard_service_requests    VIEW transformasi untuk dashboard
```

Orkestrasi: **Apache Airflow** (LocalExecutor, Docker Compose), DAG: `finpro_elvita_nyc311_daily_batch`.

### Streaming Pipeline (simulasi real-time)

```
NYC Open Data API (replay data terbaru)
        |
        v
stream_producer.py    Google Cloud Pub/Sub: finpro-elvita-nyc311-stream-topic
        |
        v
stream_consumer.py    Subscribe: finpro-elvita-nyc311-stream-sub
        |
        v
BigQuery: fact_service_requests_stream
```

*Catatan: NYC 311 tidak menyediakan feed real-time asli. Komponen streaming mensimulasikan aliran data dengan me-replay data terbaru satu per satu (jeda ~1 detik), dipublish dan dikonsumsi lewat Pub/Sub secara real-time.*

### Alerting

Setiap task Airflow yang gagal otomatis memicu `on_failure_callback` (`alert_on_pipeline_failure`) yang mencatat detail kegagalan (DAG, task, waktu, exception, link log) ke `logs/pipeline_alerts.log`.

## Data Quality

- **validate_staging** — jumlah baris staging harus sesuai hasil ekstraksi, tidak ada `unique_key` NULL, tidak ada duplikat.
- **validate_warehouse** — jumlah baris warehouse per partisi tanggal sesuai staging, tidak ada duplikat `unique_key`, kolom kunci (`unique_key`, `created_at`) tidak NULL.

## Tech Stack

| Komponen | Teknologi |
|---|---|
| Orkestrasi | Apache Airflow (Docker Compose, LocalExecutor) |
| Data Lake | Google Cloud Storage |
| Data Warehouse | BigQuery |
| Streaming | Google Cloud Pub/Sub |
| Dashboard | Looker Studio |
| Bahasa | Python 3.12, SQL (BigQuery Standard SQL) |

## Struktur Folder

```
dags/     DAG Airflow (finpro_elvita_nyc311_daily_batch.py)
sql/      SQL: create_warehouse, merge_service_requests, create_dashboard_view
src/      Script ekstraksi (extract_nyc311.py) dan streaming (stream_producer.py, stream_consumer.py)
docs/     Dokumentasi tambahan
```

## Cara Menjalankan

1. `docker compose up -d`
2. Buka Airflow UI di `http://localhost:8080`
3. Trigger DAG `finpro_elvita_nyc311_daily_batch`
4. Untuk demo streaming, di folder `src/`, jalankan di dua terminal terpisah:
   - `python stream_consumer.py`
   - `python stream_producer.py`
