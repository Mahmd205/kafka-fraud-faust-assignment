"""Create Kafka topics required by the project.

Run:
    python src/create_topics.py
"""

from __future__ import annotations

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

try:
    from .settings import BOOTSTRAP_SERVERS, PREDICTIONS_TOPIC, RAW_TOPIC
except ImportError:
    from settings import BOOTSTRAP_SERVERS, PREDICTIONS_TOPIC, RAW_TOPIC


def main() -> None:
    admin = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id="fraud-topic-admin",
    )

    topics = [
        NewTopic(name=RAW_TOPIC, num_partitions=1, replication_factor=1),
        NewTopic(name=PREDICTIONS_TOPIC, num_partitions=1, replication_factor=1),
    ]

    try:
        admin.create_topics(new_topics=topics, validate_only=False)
        print(f"Created topics: {RAW_TOPIC}, {PREDICTIONS_TOPIC}")
    except TopicAlreadyExistsError:
        print("Topics already exist. Continuing...")
    finally:
        admin.close()


if __name__ == "__main__":
    main()
