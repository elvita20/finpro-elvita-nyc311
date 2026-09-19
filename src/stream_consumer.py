import json
import os

from datetime import datetime, timezone

from dotenv import load_dotenv
from google.cloud import bigquery, pubsub_v1


# ============================================================
# CONFIGURATION
# ============================================================

DATASET_ID = "erm2-nwe9"

STREAM_SUBSCRIPTION_ID = "finpro-elvita-nyc311-stream-sub"

STAGING_DATASET = "finpro_elvita_nyc311_staging"
STAGING_TABLE = "stg_311_stream"

RUN_DURATION_SECONDS = 150


# ============================================================
# LOAD ENVIRONMENT VARIABLES
# ============================================================

load_dotenv()

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")


# ============================================================
# PHASE 2 -- MERGE FROM STAGING TO WAREHOUSE
# Same shape as merge_service_requests.sql, but reads from
# stg_311_stream and tags ingestion_source = 'streaming'.
# ============================================================

MERGE_SQL = f"""
MERGE `{GCP_PROJECT_ID}.finpro_elvita_nyc311_dw.fact_service_requests` AS target

USING
(
    WITH typed_source AS
    (
        SELECT
            CAST(unique_key AS STRING) AS unique_key,
            SAFE_CAST(created_date AS DATETIME) AS created_at,
            SAFE_CAST(closed_date AS DATETIME) AS closed_at,
            CAST(agency AS STRING) AS agency,
            CAST(agency_name AS STRING) AS agency_name,
            CAST(complaint_type AS STRING) AS complaint_type,
            CAST(descriptor AS STRING) AS descriptor,
            CAST(location_type AS STRING) AS location_type,
            CAST(incident_zip AS STRING) AS incident_zip,
            CAST(city AS STRING) AS city,
            CAST(status AS STRING) AS status,
            SAFE_CAST(due_date AS DATETIME) AS due_at,
            CAST(resolution_description AS STRING) AS resolution_description,
            SAFE_CAST(resolution_action_updated_date AS DATETIME) AS resolution_updated_at,
            CAST(community_board AS STRING) AS community_board,
            CAST(borough AS STRING) AS borough,
            CAST(open_data_channel_type AS STRING) AS open_data_channel_type,
            SAFE_CAST(latitude AS FLOAT64) AS latitude,
            SAFE_CAST(longitude AS FLOAT64) AS longitude,
            CAST(source_dataset_id AS STRING) AS source_dataset_id,
            SAFE_CAST(source_partition_date AS DATE) AS source_partition_date,
            SAFE_CAST(etl_extracted_at_utc AS TIMESTAMP) AS etl_extracted_at
        FROM `{GCP_PROJECT_ID}.{STAGING_DATASET}.{STAGING_TABLE}`
        WHERE unique_key IS NOT NULL
    )
    SELECT *
    FROM typed_source
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY unique_key ORDER BY etl_extracted_at DESC
    ) = 1
) AS source

ON target.unique_key = source.unique_key

WHEN MATCHED THEN
UPDATE SET
    created_at = source.created_at,
    closed_at = source.closed_at,
    agency = source.agency,
    agency_name = source.agency_name,
    complaint_type = source.complaint_type,
    descriptor = source.descriptor,
    location_type = source.location_type,
    incident_zip = source.incident_zip,
    city = source.city,
    status = source.status,
    due_at = source.due_at,
    resolution_description = source.resolution_description,
    resolution_updated_at = source.resolution_updated_at,
    community_board = source.community_board,
    borough = source.borough,
    open_data_channel_type = source.open_data_channel_type,
    latitude = source.latitude,
    longitude = source.longitude,
    source_dataset_id = source.source_dataset_id,
    source_partition_date = source.source_partition_date,
    etl_extracted_at = source.etl_extracted_at,
    etl_loaded_at = CURRENT_TIMESTAMP(),
    ingestion_source = 'streaming'

WHEN NOT MATCHED THEN
INSERT
(
    unique_key, created_at, closed_at, agency, agency_name,
    complaint_type, descriptor, location_type, incident_zip, city,
    status, due_at, resolution_description, resolution_updated_at,
    community_board, borough, open_data_channel_type,
    latitude, longitude, source_dataset_id, source_partition_date,
    etl_extracted_at, etl_loaded_at, ingestion_source
)
VALUES
(
    source.unique_key, source.created_at, source.closed_at, source.agency, source.agency_name,
    source.complaint_type, source.descriptor, source.location_type, source.incident_zip, source.city,
    source.status, source.due_at, source.resolution_description, source.resolution_updated_at,
    source.community_board, source.borough, source.open_data_channel_type,
    source.latitude, source.longitude, source.source_dataset_id, source.source_partition_date,
    source.etl_extracted_at, CURRENT_TIMESTAMP(), 'streaming'
);
"""


# ============================================================
# PHASE 1 -- LAND RAW MESSAGE INTO STAGING
# ============================================================

def make_callback(bq_client, staging_table_ref):

    def callback(message):

        try:
            record = json.loads(
                message.data.decode("utf-8")
            )

            streamed_at_utc = record.get(
                "streamed_at_utc"
            ) or datetime.now(timezone.utc).isoformat()

            created_date = record.get("created_date")
            source_partition_date = (
                created_date[:10] if created_date else None
            )

            row = {
                "unique_key": record.get("unique_key"),
                "created_date": created_date,
                "closed_date": record.get("closed_date"),
                "agency": record.get("agency"),
                "agency_name": record.get("agency_name"),
                "complaint_type": record.get("complaint_type"),
                "descriptor": record.get("descriptor"),
                "location_type": record.get("location_type"),
                "incident_zip": record.get("incident_zip"),
                "city": record.get("city"),
                "status": record.get("status"),
                "due_date": record.get("due_date"),
                "resolution_description": record.get("resolution_description"),
                "resolution_action_updated_date": record.get(
                    "resolution_action_updated_date"
                ),
                "community_board": record.get("community_board"),
                "borough": record.get("borough"),
                "open_data_channel_type": record.get("open_data_channel_type"),
                "latitude": record.get("latitude"),
                "longitude": record.get("longitude"),
                "source_dataset_id": DATASET_ID,
                "source_partition_date": source_partition_date,
                "etl_extracted_at_utc": streamed_at_utc,
            }

            errors = bq_client.insert_rows_json(
                staging_table_ref,
                [row],
            )

            if errors:
                print(f"Staging insert errors: {errors}")
                message.nack()
                return

            print(
                f"Staged unique_key={row['unique_key']} "
                f"complaint_type={row['complaint_type']}"
            )

            message.ack()

        except Exception as error:
            print(f"Failed to process message: {error}")
            message.nack()

    return callback


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 70)
    print("FINPRO ELVITA - NYC 311 STREAM CONSUMER")
    print("=" * 70)

    bq_client = bigquery.Client(project=GCP_PROJECT_ID)

    staging_table_ref = (
        f"{GCP_PROJECT_ID}.{STAGING_DATASET}.{STAGING_TABLE}"
    )

    subscriber = pubsub_v1.SubscriberClient()

    subscription_path = subscriber.subscription_path(
        GCP_PROJECT_ID,
        STREAM_SUBSCRIPTION_ID,
    )

    callback = make_callback(bq_client, staging_table_ref)

    streaming_pull_future = subscriber.subscribe(
        subscription_path,
        callback=callback,
    )

    print(f"Listening on {subscription_path}")
    print(f"Landing raw messages into: {staging_table_ref}")
    print(f"Will run for {RUN_DURATION_SECONDS} seconds...")
    print()

    with subscriber:
        try:
            streaming_pull_future.result(
                timeout=RUN_DURATION_SECONDS
            )
        except Exception:
            streaming_pull_future.cancel()
            streaming_pull_future.result()

    print()
    print("=" * 70)
    print("PHASE 2: Merging staging -> fact_service_requests")
    print("=" * 70)

    query_job = bq_client.query(MERGE_SQL)
    query_job.result()

    print(
        f"Merge complete. "
        f"{query_job.num_dml_affected_rows} row(s) affected "
        f"in fact_service_requests."
    )

    print()
    print("=" * 70)
    print("Consumer stopped")
    print("=" * 70)


if __name__ == "__main__":
    main()