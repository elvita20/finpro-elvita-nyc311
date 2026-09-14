import logging
from datetime import datetime
from pathlib import Path

import pendulum

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryInsertJobOperator,
)

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

from src.extract_nyc311 import (
    extract_nyc311,
    upload_to_gcs,
)


# ============================================================
# FINPRO ELVITA - NYC 311 DAILY BATCH PIPELINE
#
# Flow:
#
# NYC Open Data API
#       |
#       v
# extract_to_gcs
#       |
#       v
# Google Cloud Storage
#       |
#       v
# load_gcs_to_staging
#       |
#       v
# validate_staging
#       |
#       v
# ensure_warehouse_table
#       |
#       v
# merge_to_warehouse
#       |
#       v
# validate_warehouse
#
# ============================================================


# ============================================================
# GCP CONFIGURATION
# ============================================================

PROJECT_ID = "jcdeah-009"

REGION = "asia-southeast1"

GCS_BUCKET = (
    "jcdeah-009-nyc311-datalake-elvita"
)

GCS_RAW_PREFIX = (
    "raw/finpro_elvita/nyc311"
)


# ============================================================
# BIGQUERY CONFIGURATION
# ============================================================

STAGING_DATASET = (
    "finpro_elvita_nyc311_staging"
)

STAGING_TABLE = (
    "stg_311_daily"
)

WAREHOUSE_DATASET = (
    "finpro_elvita_nyc311_dw"
)

WAREHOUSE_TABLE = (
    "fact_service_requests"
)


# ============================================================
# SQL FILES
# ============================================================

CREATE_WAREHOUSE_SQL_PATH = Path(
    "/opt/airflow/sql/create_warehouse.sql"
)

MERGE_WAREHOUSE_SQL_PATH = Path(
    "/opt/airflow/sql/merge_service_requests.sql"
)


CREATE_WAREHOUSE_SQL = (
    CREATE_WAREHOUSE_SQL_PATH.read_text(
        encoding="utf-8"
    )
)

MERGE_WAREHOUSE_SQL = (
    MERGE_WAREHOUSE_SQL_PATH.read_text(
        encoding="utf-8"
    )
)


# ============================================================
# ALERTING
# ============================================================

ALERT_LOG_PATH = Path(
    "/opt/airflow/logs/pipeline_alerts.log"
)

logger = logging.getLogger("airflow.task")


def alert_on_pipeline_failure(context):

    dag_id = context["dag"].dag_id

    task_id = context["task_instance"].task_id

    logical_date = context.get(
        "logical_date",
        context.get("execution_date"),
    )

    exception = context.get("exception")

    log_url = context["task_instance"].log_url

    message = (
        "[PIPELINE ALERT] "
        f"DAG={dag_id} "
        f"TASK={task_id} "
        f"LOGICAL_DATE={logical_date} "
        f"EXCEPTION={exception} "
        f"LOG_URL={log_url}"
    )

    logger.error(message)

    try:
        with ALERT_LOG_PATH.open(
            "a",
            encoding="utf-8",
        ) as f:
            f.write(message + "\n")

    except Exception as write_error:
        logger.error(
            f"Failed to write alert log: {write_error}"
        )


# ============================================================
# DAG DEFINITION
# ============================================================

@dag(
    dag_id="finpro_elvita_nyc311_daily_batch",

    description=(
        "FinPro Elvita NYC311 batch pipeline: "
        "API -> GCS -> BigQuery Staging -> Warehouse"
    ),

    # Untuk sekarang manual.
    # Setelah testing sukses kita ubah menjadi daily schedule.
    schedule=None,

    start_date=pendulum.datetime(
        2026,
        1,
        1,
        tz="America/New_York",
    ),

    catchup=False,

    max_active_runs=1,

    is_paused_upon_creation=False,

    default_args={
        "on_failure_callback": alert_on_pipeline_failure,
    },

    params={
        "target_date": Param(
            default="2026-08-30",
            type="string",
            description=(
                "NYC311 Created Date "
                "in YYYY-MM-DD format"
            ),
        ),
    },

    tags=[
        "finpro-elvita",
        "nyc311",
        "batch",
        "gcp",
        "bigquery",
    ],
)
def finpro_elvita_nyc311_daily_batch():

    # ========================================================
    # TASK 1
    # EXTRACT NYC311 API -> GCS DATA LAKE
    # ========================================================

    @task(
        task_id="extract_to_gcs"
    )
    def extract_to_gcs():

        context = get_current_context()

        target_date_string = (
            context["params"]["target_date"]
        )

        target_date = datetime.strptime(
            target_date_string,
            "%Y-%m-%d",
        ).date()

        print(
            f"Processing NYC311 data "
            f"for {target_date_string}"
        )

        # ----------------------------------------------------
        # Extract NYC311 data from Socrata API
        # ----------------------------------------------------

        local_file, total_rows = (
            extract_nyc311(
                target_date
            )
        )

        # ----------------------------------------------------
        # Upload raw file into GCS Data Lake
        # ----------------------------------------------------

        gcs_uri = upload_to_gcs(
            local_file,
            target_date,
        )

        object_name = (
            f"{GCS_RAW_PREFIX}/"
            f"created_date="
            f"{target_date_string}/"
            f"data.jsonl.gz"
        )

        print(
            f"Extracted rows : {total_rows:,}"
        )

        print(
            f"GCS object     : {object_name}"
        )

        # ----------------------------------------------------
        # Returned dictionary becomes Airflow XCom
        # ----------------------------------------------------

        return {
            "target_date":
                target_date_string,

            "row_count":
                total_rows,

            "object_name":
                object_name,

            "gcs_uri":
                gcs_uri,
        }


    extract_task = extract_to_gcs()


    # ========================================================
    # TASK 2
    # GCS DATA LAKE -> BIGQUERY STAGING
    # ========================================================

    load_to_staging = (
        GCSToBigQueryOperator(

            task_id="load_gcs_to_staging",

            bucket=GCS_BUCKET,

            source_objects=[
                (
                    "{{ "
                    "ti.xcom_pull("
                    "task_ids='extract_to_gcs'"
                    ")['object_name'] "
                    "}}"
                )
            ],

            destination_project_dataset_table=(
                f"{PROJECT_ID}:{STAGING_DATASET}.{STAGING_TABLE}"
            ),

            project_id=PROJECT_ID,

            source_format="NEWLINE_DELIMITED_JSON",

            autodetect=True,

            write_disposition="WRITE_TRUNCATE",

            create_disposition="CREATE_IF_NEEDED",

            location=REGION,

            gcp_conn_id="google_cloud_default",
        )
    )


    # ========================================================
    # TASK 3
    # STAGING DATA QUALITY VALIDATION
    # ========================================================

    validate_staging = (
        BigQueryCheckOperator(

            task_id="validate_staging",

            sql=f"""
            SELECT

                COUNT(*) > 0

                AND COUNT(*) =
                    {{{{
                        ti.xcom_pull(
                            task_ids='extract_to_gcs'
                        )['row_count']
                    }}}}

                AND COUNTIF(
                    unique_key IS NULL
                ) = 0

                AND COUNT(*) =
                    COUNT(
                        DISTINCT
                        CAST(
                            unique_key
                            AS STRING
                        )
                    )

            FROM
                `{PROJECT_ID}.{STAGING_DATASET}.{STAGING_TABLE}`
            """,

            project_id=PROJECT_ID,

            use_legacy_sql=False,

            location=REGION,

            gcp_conn_id=(
                "google_cloud_default"
            ),
        )
    )


    # ========================================================
    # TASK 4
    # ENSURE DATA WAREHOUSE TABLE EXISTS
    # ========================================================

    ensure_warehouse_table = (
        BigQueryInsertJobOperator(

            task_id=(
                "ensure_warehouse_table"
            ),

            configuration={
                "query": {
                    "query":
                        CREATE_WAREHOUSE_SQL,

                    "useLegacySql":
                        False,
                }
            },

            location=REGION,

            gcp_conn_id=(
                "google_cloud_default"
            ),
        )
    )


    # ========================================================
    # TASK 5
    # STAGING -> WAREHOUSE MERGE / UPSERT
    # ========================================================

    merge_to_warehouse = (
        BigQueryInsertJobOperator(

            task_id="merge_to_warehouse",

            configuration={
                "query": {
                    "query":
                        MERGE_WAREHOUSE_SQL,

                    "useLegacySql":
                        False,
                }
            },

            location=REGION,

            gcp_conn_id=(
                "google_cloud_default"
            ),
        )
    )


    # ========================================================
    # TASK 6
    # WAREHOUSE DATA QUALITY VALIDATION
    # ========================================================

    validate_warehouse = (
        BigQueryCheckOperator(

            task_id="validate_warehouse",

            sql=f"""
            WITH staging AS
            (
                SELECT
                    COUNT(
                        DISTINCT
                        CAST(
                            unique_key
                            AS STRING
                        )
                    ) AS row_count

                FROM
                    `{PROJECT_ID}.{STAGING_DATASET}.{STAGING_TABLE}`
            ),

            warehouse_partition AS
            (
                SELECT
                    COUNT(*) AS row_count

                FROM
                    `{PROJECT_ID}.{WAREHOUSE_DATASET}.{WAREHOUSE_TABLE}`

                WHERE
                    source_partition_date =
                    DATE(
                        '{{{{ params.target_date }}}}'
                    )
            ),

            warehouse_quality AS
            (
                SELECT

                    COUNT(*) AS total_rows,

                    COUNT(
                        DISTINCT unique_key
                    ) AS distinct_keys,

                    COUNTIF(
                        unique_key IS NULL
                    ) AS null_keys,

                    COUNTIF(
                        created_at IS NULL
                    ) AS null_created_at

                FROM
                    `{PROJECT_ID}.{WAREHOUSE_DATASET}.{WAREHOUSE_TABLE}`
            )

            SELECT

                staging.row_count > 0

                AND staging.row_count =
                    warehouse_partition.row_count

                AND warehouse_quality.total_rows =
                    warehouse_quality.distinct_keys

                AND warehouse_quality.null_keys = 0

                AND warehouse_quality.null_created_at = 0

            FROM
                staging

            CROSS JOIN
                warehouse_partition

            CROSS JOIN
                warehouse_quality
            """,

            use_legacy_sql=False,

            location=REGION,

            gcp_conn_id=(
                "google_cloud_default"
            ),
        )
    )


    # ========================================================
    # TASK DEPENDENCIES
    # ========================================================

    (
        extract_task

        >> load_to_staging

        >> validate_staging

        >> ensure_warehouse_table

        >> merge_to_warehouse

        >> validate_warehouse
    )


# ============================================================
# REGISTER DAG
# ============================================================

finpro_elvita_nyc311_daily_batch()