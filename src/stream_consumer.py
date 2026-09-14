import json
import os

from dotenv import load_dotenv
from google.cloud import bigquery, pubsub_v1


# ============================================================
# CONFIGURATION
# ============================================================

STREAM_SUBSCRIPTION_ID = "finpro-elvita-nyc311-stream-sub"

STREAM_TABLE = (
    "finpro_elvita_nyc311_dw."
    "fact_service_requests_stream"
)

RUN_DURATION_SECONDS = 150


# ============================================================
# LOAD ENVIRONMENT VARIABLES
# ============================================================

load_dotenv()

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")


# ============================================================
# HANDLE INCOMING MESSAGE
# ============================================================

def make_callback(bq_client, table_ref):

    def callback(message):

        try:
            record = json.loads(
                message.data.decode("utf-8")
            )

            row = {
                "unique_key":
                    record.get("unique_key"),

                "complaint_type":
                    record.get("complaint_type"),

                "agency":
                    record.get("agency"),

                "agency_name":
                    record.get("agency_name"),

                "borough":
                    record.get("borough"),

                "status":
                    record.get("status"),

                "created_at":
                    record.get("created_date"),

                "streamed_at_utc":
                    record.get("streamed_at_utc"),

                "raw_payload":
                    json.dumps(
                        record,
                        ensure_ascii=False,
                    ),
            }

            errors = bq_client.insert_rows_json(
                table_ref,
                [row],
            )

            if errors:
                print(
                    f"BigQuery insert errors: {errors}"
                )
                message.nack()
                return

            print(
                f"Inserted unique_key="
                f"{row['unique_key']} "
                f"complaint_type="
                f"{row['complaint_type']}"
            )

            message.ack()

        except Exception as error:
            print(
                f"Failed to process message: {error}"
            )
            message.nack()

    return callback


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 70)
    print("FINPRO ELVITA - NYC 311 STREAM CONSUMER")
    print("=" * 70)

    bq_client = bigquery.Client(
        project=GCP_PROJECT_ID
    )

    table_ref = (
        f"{GCP_PROJECT_ID}.{STREAM_TABLE}"
    )

    subscriber = pubsub_v1.SubscriberClient()

    subscription_path = (
        subscriber.subscription_path(
            GCP_PROJECT_ID,
            STREAM_SUBSCRIPTION_ID,
        )
    )

    callback = make_callback(
        bq_client,
        table_ref,
    )

    streaming_pull_future = (
        subscriber.subscribe(
            subscription_path,
            callback=callback,
        )
    )

    print(
        f"Listening on {subscription_path}"
    )

    print(
        f"Will run for "
        f"{RUN_DURATION_SECONDS} seconds..."
    )

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
    print("Consumer stopped")
    print("=" * 70)


if __name__ == "__main__":
    main()
