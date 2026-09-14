import json
import os
import time

from datetime import datetime, timezone

import requests

from dotenv import load_dotenv
from google.cloud import pubsub_v1

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

STREAM_TOPIC_ID = "finpro-elvita-nyc311-stream-topic"

STREAM_MESSAGE_COUNT = 100

STREAM_DELAY_SECONDS = 1.0


# ============================================================
# LOAD ENVIRONMENT VARIABLES
# ============================================================

load_dotenv()

SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")


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
# FETCH RECENT NYC311 RECORDS TO REPLAY
# ============================================================

def fetch_recent_records(limit):

    headers = {
        "Accept": "application/json",
        "X-App-Token": SOCRATA_APP_TOKEN,
    }

    params = {
        "$order": "created_date DESC",
        "$limit": limit,
    }

    session = create_http_session()

    response = session.get(
        SOCRATA_API_URL,
        params=params,
        headers=headers,
        timeout=(15, 60),
    )

    response.raise_for_status()

    return response.json()


# ============================================================
# STREAM RECORDS TO PUB/SUB
# ============================================================

def main():

    print("=" * 70)
    print("FINPRO ELVITA - NYC 311 STREAM PRODUCER")
    print("=" * 70)

    print(
        f"Fetching {STREAM_MESSAGE_COUNT} "
        f"recent records..."
    )

    records = fetch_recent_records(
        STREAM_MESSAGE_COUNT
    )

    print(f"Fetched {len(records):,} records")

    print(
        f"Publishing to topic: {STREAM_TOPIC_ID}"
    )

    print()

    publisher = pubsub_v1.PublisherClient()

    topic_path = publisher.topic_path(
        GCP_PROJECT_ID,
        STREAM_TOPIC_ID,
    )

    for index, record in enumerate(
        records,
        start=1,
    ):

        record["source_dataset_id"] = DATASET_ID

        record["streamed_at_utc"] = (
            datetime.now(timezone.utc)
            .isoformat()
        )

        payload = json.dumps(
            record,
            ensure_ascii=False,
        ).encode("utf-8")

        future = publisher.publish(
            topic_path,
            data=payload,
        )

        message_id = future.result()

        print(
            f"[{index}/{len(records)}] "
            f"Published unique_key="
            f"{record.get('unique_key')} "
            f"message_id={message_id}"
        )

        time.sleep(STREAM_DELAY_SECONDS)

    print()
    print("=" * 70)
    print("Streaming complete")
    print("=" * 70)


if __name__ == "__main__":
    main()
