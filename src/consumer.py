"""Output consumer that prints colored predictions from Kafka.

Run:
    python src/consumer.py
"""

from __future__ import annotations

import json

from kafka import KafkaConsumer

try:
    from colorama import Fore, Style, init

    init(autoreset=True)
except ImportError:
    # Fallback if colorama is not installed
    class Fore:
        RED = ""
        GREEN = ""
        YELLOW = ""
        CYAN = ""
        WHITE = ""

    class Style:
        BRIGHT = ""
        RESET_ALL = ""

try:
    from .settings import BOOTSTRAP_SERVERS, PREDICTIONS_TOPIC
except ImportError:
    from settings import BOOTSTRAP_SERVERS, PREDICTIONS_TOPIC


def format_prediction(result: dict) -> str:
    label = str(result.get("prediction_label", "UNKNOWN")).upper()
    probability = result.get("fraud_probability")
    actual = result.get("actual_class")
    amount = result.get("amount")
    tx_id = result.get("transaction_id")

    if label == "FRAUD" or result.get("predicted_class") == 1:
        color = Fore.RED + Style.BRIGHT
        icon = "🚨"
    else:
        color = Fore.GREEN + Style.BRIGHT
        icon = "✅"

    return (
        f"{color}{icon} Transaction {tx_id} | "
        f"Prediction: {label:<6} | "
        f"Fraud probability: {probability} | "
        f"Actual class: {actual} | "
        f"Amount: {amount}"
        f"{Style.RESET_ALL}"
    )


def main() -> None:
    consumer = KafkaConsumer(
        PREDICTIONS_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="fraud-output-console-consumer",
        key_deserializer=lambda key: key.decode("utf-8") if key else None,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    print(
        Fore.CYAN
        + Style.BRIGHT
        + f"Listening for predictions on topic '{PREDICTIONS_TOPIC}'..."
        + Style.RESET_ALL
    )

    try:
        for message in consumer:
            result = message.value
            print(format_prediction(result))
    except KeyboardInterrupt:
        print(Fore.YELLOW + "Consumer stopped by user." + Style.RESET_ALL)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()