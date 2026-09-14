import argparse
import gzip
import json
import os
import time

from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

from dotenv import load_dotenv
from google.cloud import storage

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry



# ============================================================
# CONFIGURATION
# ============================================================

DATASET_ID = "erm2-nwe9"

SOCRATA_API_URL = (
    "https://data.cityofnewyork.us/"
    "resource/erm2-nwe9.json"
)

PAGE_SIZE = 50000


# ============================================================
# LOAD ENVIRONMENT VARIABLES
# ============================================================

load_dotenv()

SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

GCS_RAW_PREFIX = os.getenv(
    "GCS_RAW_PREFIX",
    "raw/finpro_elvita/nyc311",
)


def create_http_session():

    retry_strategy = Retry(
        total=5,
        connect=3,
        read=3,
        status=5,

        backoff_factor=5,

        status_forcelist=[
            429,
            500,
            502,
            503,
            504,
        ],

        allowed_methods=[
            "GET",
        ],

        respect_retry_after_header=True,
        raise_on_status=False,
    )

    adapter = HTTPAdapter(
        max_retries=retry_strategy
    )

    session = requests.Session()

    session.mount(
        "https://",
        adapter,
    )

    return session


# ============================================================
# ENVIRONMENT VALIDATION
# ============================================================

def validate_environment():
    required_variables = {
        "SOCRATA_APP_TOKEN": SOCRATA_APP_TOKEN,
        "GCP_PROJECT_ID": GCP_PROJECT_ID,
        "GCS_BUCKET_NAME": GCS_BUCKET_NAME,
    }

    missing = [
        name
        for name, value in required_variables.items()
        if not value
    ]

    if missing:
        raise RuntimeError(
            "Missing environment variables: "
            + ", ".join(missing)
        )


# ============================================================
# BUILD SOQL QUERY
# ============================================================

def build_query(target_date):
    next_date = target_date + timedelta(days=1)

    start_datetime = (
        f"{target_date.isoformat()}T00:00:00.000"
    )

    end_datetime = (
        f"{next_date.isoformat()}T00:00:00.000"
    )

    query = f"""
        SELECT *
        WHERE created_date >= '{start_datetime}'
          AND created_date < '{end_datetime}'
        ORDER BY created_date ASC, unique_key ASC
    """

    return " ".join(query.split())


# ============================================================
# EXTRACT DATA FROM NYC OPEN DATA
# ============================================================

def extract_nyc311(target_date):

    next_date = target_date + timedelta(days=1)

    start_datetime = (
        f"{target_date.isoformat()}T00:00:00.000"
    )

    end_datetime = (
        f"{next_date.isoformat()}T00:00:00.000"
    )

    local_directory = (
        Path("tmp")
        / "finpro_elvita"
        / "nyc311"
        / f"created_date={target_date.isoformat()}"
    )

    local_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    local_file = (
        local_directory
        / "data.jsonl.gz"
    )

    headers = {
        "Accept": "application/json",
        "X-App-Token": SOCRATA_APP_TOKEN,
    }

    session = create_http_session()

    total_rows = 0
    offset = 0
    page_number = 1

    print("=" * 70)
    print("FINPRO ELVITA - NYC 311 EXTRACT")
    print("=" * 70)

    print(f"Dataset   : {DATASET_ID}")
    print(f"Date      : {target_date}")
    print(f"Page size : {PAGE_SIZE}")
    print()

    with gzip.open(
        local_file,
        "wt",
        encoding="utf-8",
    ) as output_file:

        while True:

            params = {

                "$where": (
                    f"created_date >= "
                    f"'{start_datetime}' "
                    f"AND created_date < "
                    f"'{end_datetime}'"
                ),

                "$order": (
                    "created_date ASC, "
                    "unique_key ASC"
                ),

                "$limit": PAGE_SIZE,

                "$offset": offset,
            }

            print(
                f"Downloading page "
                f"{page_number}..."
            )

            response = session.get(
                SOCRATA_API_URL,
                params=params,
                headers=headers,
                timeout=(15, 300),
            )

            if response.status_code == 429:

                retry_after = (
                    response.headers.get(
                        "Retry-After"
                    )
                )

                raise RuntimeError(
                    "Socrata rate limit reached. "
                    f"Retry-After={retry_after}"
                )

            response.raise_for_status()

            rows = response.json()

            if not rows:
                break

            extracted_at = (
                datetime.now(timezone.utc)
                .isoformat()
            )

            for row in rows:

                row["source_dataset_id"] = (
                    DATASET_ID
                )

                row["source_partition_date"] = (
                    target_date.isoformat()
                )

                row["etl_extracted_at_utc"] = (
                    extracted_at
                )

                output_file.write(
                    json.dumps(
                        row,
                        ensure_ascii=False,
                    )
                )

                output_file.write("\n")

            row_count = len(rows)

            total_rows += row_count

            print(
                f"Page {page_number}: "
                f"{row_count:,} rows"
            )

            print(
                f"Total: "
                f"{total_rows:,} rows"
            )

            # Daily dataset normally finishes
            # in one request.
            if row_count < PAGE_SIZE:
                break

            offset += PAGE_SIZE
            page_number += 1

            # Be courteous to the API
            time.sleep(2)

    if total_rows == 0:

        if local_file.exists():
            local_file.unlink()

        raise RuntimeError(
            f"No records found for "
            f"{target_date}"
        )

    print()
    print("=" * 70)
    print(
        f"Extraction complete: "
        f"{total_rows:,} rows"
    )
    print(
        f"Local file: {local_file}"
    )
    print("=" * 70)

    return local_file, total_rows


# ============================================================
# UPLOAD FILE TO GOOGLE CLOUD STORAGE
# ============================================================

def upload_to_gcs(
    local_file,
    target_date,
):

    object_name = (
        f"{GCS_RAW_PREFIX}/"
        f"created_date={target_date.isoformat()}/"
        f"data.jsonl.gz"
    )

    print()
    print("=" * 70)
    print("UPLOAD TO GCS")
    print("=" * 70)
    print(
        f"Bucket : {GCS_BUCKET_NAME}"
    )
    print(
        f"Object : {object_name}"
    )

    storage_client = storage.Client(
        project=GCP_PROJECT_ID
    )

    bucket = storage_client.bucket(
        GCS_BUCKET_NAME
    )

    blob = bucket.blob(
        object_name
    )

    blob.upload_from_filename(
        str(local_file),
        content_type="application/gzip",
    )

    gcs_uri = (
        f"gs://{GCS_BUCKET_NAME}/"
        f"{object_name}"
    )

    print()
    print("Upload successful")
    print(gcs_uri)

    return gcs_uri


# ============================================================
# MAIN
# ============================================================

def main():

    parser = argparse.ArgumentParser(
        description=(
            "Extract NYC 311 data for one day "
            "and upload it to GCS."
        )
    )

    parser.add_argument(
        "--date",
        required=True,
        help="YYYY-MM-DD",
    )

    args = parser.parse_args()

    try:

        target_date = datetime.strptime(
            args.date,
            "%Y-%m-%d",
        ).date()

    except ValueError:

        raise SystemExit(
            "Invalid date. "
            "Use YYYY-MM-DD."
        )

    validate_environment()

    local_file, total_rows = (
        extract_nyc311(
            target_date
        )
    )

    gcs_uri = upload_to_gcs(
        local_file,
        target_date,
    )

    print()
    print("=" * 70)
    print("PIPELINE SUMMARY")
    print("=" * 70)
    print(
        f"Target date : {target_date}"
    )
    print(
        f"Rows        : {total_rows:,}"
    )
    print(
        f"Local file  : {local_file}"
    )
    print(
        f"GCS URI     : {gcs_uri}"
    )
    print("=" * 70)


if __name__ == "__main__":
    main()