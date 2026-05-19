# 2-3 Minute Video Demo Script

Use this script while recording your assignment demo.

## Opening

This project is a real-time credit card fraud detection pipeline using Apache Kafka and Faust. The producer reads rows from the Credit Card Fraud Detection dataset and sends each row as a live JSON event to the Kafka `raw-data` topic.

## Terminal 1: Faust Streams Processor

This terminal runs the Faust Streams processor. It consumes from `raw-data`, loads the offline trained Logistic Regression model, predicts whether each transaction is fraudulent, and publishes the result to the `predictions` topic.

Command:

```bash
faust -A src.app worker -l info
```

## Terminal 2: Output Consumer

This terminal runs the output consumer. It reads from the `predictions` topic and prints each prediction in a readable format.

Command:

```bash
python src/consumer.py
```

## Terminal 3: Producer

This terminal runs the producer. It replays the dataset at approximately one row per second to simulate real-time streaming.

Command:

```bash
python src/producer.py --rate 1
```

## Closing

As shown in the consumer terminal, predictions are printed live as new transactions arrive. This demonstrates the complete real-time pipeline: dataset rows are streamed to Kafka, processed by Faust, classified by a trained ML model, and published to an output topic in real time.
