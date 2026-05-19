"""Faust Streams processor for real-time fraud prediction.

Run:
    faust -A src.app worker -l info
"""

from __future__ import annotations

from typing import Any

import faust
import joblib
import pandas as pd

try:
    from .settings import BOOTSTRAP_SERVERS, FEATURE_COLUMNS, MODEL_PATH, PREDICTIONS_TOPIC, RAW_TOPIC
except ImportError:
    from settings import BOOTSTRAP_SERVERS, FEATURE_COLUMNS, MODEL_PATH, PREDICTIONS_TOPIC, RAW_TOPIC

app = faust.App(
    "fraud-faust-streams-app",
    broker=f"kafka://{BOOTSTRAP_SERVERS}",
    value_serializer="json",
)

raw_topic = app.topic(
    RAW_TOPIC,
    key_type=str,
    value_serializer="json",
)

predictions_topic = app.topic(
    PREDICTIONS_TOPIC,
    key_type=str,
    value_serializer="json",
)

_model_bundle: dict[str, Any] | None = None


def load_model_bundle() -> dict[str, Any]:
    global _model_bundle
    if _model_bundle is None:
        if not MODEL_PATH.exists():
            raise FileNotFoundError(
                f"Model not found at {MODEL_PATH}. Run: python src/train_model.py"
            )
        _model_bundle = joblib.load(MODEL_PATH)
        print(f"Loaded model from {MODEL_PATH}")
    return _model_bundle


def predict_event(event: dict[str, Any]) -> dict[str, Any]:
    bundle = load_model_bundle()
    model = bundle["model"]
    feature_columns = bundle.get("feature_columns", FEATURE_COLUMNS)
    threshold = float(bundle.get("threshold", 0.5))

    missing = [col for col in feature_columns if col not in event]
    if missing:
        raise ValueError(f"Event is missing required feature(s): {missing}")

    x = pd.DataFrame([{col: event[col] for col in feature_columns}])

    if hasattr(model, "predict_proba"):
        fraud_probability = float(model.predict_proba(x)[0][1])
        predicted_class = int(fraud_probability >= threshold)
    else:
        predicted_class = int(model.predict(x)[0])
        fraud_probability = float(predicted_class)

    return {
        "transaction_id": event.get("transaction_id"),
        "predicted_class": predicted_class,
        "prediction_label": "FRAUD" if predicted_class == 1 else "NORMAL",
        "fraud_probability": round(fraud_probability, 6),
        "actual_class": event.get("actual_class"),
        "amount": event.get("Amount"),
        "time": event.get("Time"),
    }


@app.agent(raw_topic)
async def fraud_prediction_stream(records):
    async for event in records:
        try:
            prediction = predict_event(event)
            key = str(prediction.get("transaction_id"))
            await predictions_topic.send(key=key, value=prediction)
            print(
                f"Processed {key}: {prediction['prediction_label']} "
                f"prob={prediction['fraud_probability']}"
            )
        except Exception as exc:
            print(f"Error processing event: {exc}; event={event}")
