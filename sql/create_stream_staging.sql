CREATE TABLE IF NOT EXISTS
`jcdeah-009.finpro_elvita_nyc311_staging.stg_311_stream`
(
    unique_key STRING,
    created_date STRING,
    closed_date STRING,
    agency STRING,
    agency_name STRING,
    complaint_type STRING,
    descriptor STRING,
    location_type STRING,
    incident_zip STRING,
    city STRING,
    status STRING,
    due_date STRING,
    resolution_description STRING,
    resolution_action_updated_date STRING,
    community_board STRING,
    borough STRING,
    open_data_channel_type STRING,
    latitude STRING,
    longitude STRING,
    source_dataset_id STRING,
    source_partition_date STRING,
    etl_extracted_at_utc STRING
)
