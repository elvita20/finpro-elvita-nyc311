CREATE TABLE IF NOT EXISTS
`jcdeah-009.finpro_elvita_nyc311_dw.fact_service_requests`
(
    unique_key STRING NOT NULL,

    created_at DATETIME,
    closed_at DATETIME,

    agency STRING,
    agency_name STRING,

    complaint_type STRING,
    descriptor STRING,

    location_type STRING,
    incident_zip STRING,
    city STRING,

    status STRING,

    due_at DATETIME,

    resolution_description STRING,
    resolution_updated_at DATETIME,

    community_board STRING,
    borough STRING,

    open_data_channel_type STRING,

    latitude FLOAT64,
    longitude FLOAT64,

    source_dataset_id STRING,
    source_partition_date DATE,
    etl_extracted_at TIMESTAMP,
    etl_loaded_at TIMESTAMP
)

PARTITION BY DATE(created_at)

CLUSTER BY
    borough,
    agency,
    complaint_type,
    status;