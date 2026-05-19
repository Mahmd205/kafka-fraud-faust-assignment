"""Kafka producer that replays credit card transactions as live JSON events.

Run:
    python src/producer.py --rate 1
"""

from __future__ import annotations

import argparse
import json
import time
from typing import Any

import pandas as pd
from kafka import KafkaProducer

try:
    from .settings import BOOTSTRAP_SERVERS, DATA_PATH, FEATURE_COLUMNS, RAW_TOPIC, TARGET_COLUMN
except ImportError:
    from settings import BOOTSTRAP_SERVERS, DATA_PATH, FEATURE_COLUMNS, RAW_TOPIC, TARGET_COLUMN


def make_json_safe(value: Any) -> Any:
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    return value


def row_to_event(index: int, row: pd.Series, include_label: bool = True) -> dict[str, Any]:
    event = {
        "transaction_id": f"tx_{index:06d}",
    }

    for col in FEATURE_COLUMNS:
        event[col] = make_json_safe(row[col])

    if include_label and TARGET_COLUMN in row:
        event["actual_class"] = int(row[TARGET_COLUMN])

    return event


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay credit card rows into Kafka.")
    parser.add_argument("--rate", type=float, default=1.0, help="Rows per second. Default: 1")
    parser.add_argument("--limit", type=int, default=0, help="Maximum rows to send. 0 means all rows.")
    parser.add_argument("--start", type=int, default=0, help="Starting row index.")
    parser.add_argument(
        "--no-label",
        action="store_true",
        help="Do not include the actual Class label in the streamed event.",
    )
    args = parser.parse_args()

    if args.rate <= 0:
        raise ValueError("--rate must be greater than zero")

    if not DATA_PATH.exists():
        raise FileNotFoundError(
            f"Dataset not found at {DATA_PATH}. Download creditcard.csv and place it in the data folder."
        )

    df = pd.read_csv(DATA_PATH)
    if args.start > 0:
        df = df.iloc[args.start:]
    if args.limit > 0:
        df = df.head(args.limit)

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        linger_ms=10,
    )

    delay_seconds = 1.0 / args.rate
    print(f"Producing to topic '{RAW_TOPIC}' at about {args.rate} row(s)/second")

    try:
        for index, row in df.iterrows():
            event = row_to_event(index, row, include_label=not args.no_label)
            key = event["transaction_id"]
            producer.send(RAW_TOPIC, key=key, value=event)
            producer.flush()
            print(f"Sent {key} | actual_class={event.get('actual_class', 'hidden')}")
            time.sleep(delay_seconds)
    except KeyboardInterrupt:
        print("Producer stopped by user.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
