"""Train the offline fraud detection model and save it for the Faust processor.

Run:
    python src/train_model.py
"""

from __future__ import annotations

import json
from pathlib import Path

import joblib
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix, f1_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

try:
    from .settings import DATA_PATH, FEATURE_COLUMNS, MODEL_PATH, TARGET_COLUMN
except ImportError:
    from settings import DATA_PATH, FEATURE_COLUMNS, MODEL_PATH, TARGET_COLUMN


def validate_dataset(df: pd.DataFrame) -> None:
    missing = [col for col in FEATURE_COLUMNS + [TARGET_COLUMN] if col not in df.columns]
    if missing:
        raise ValueError(
            "The dataset is missing required columns: " + ", ".join(missing)
        )


def main() -> None:
    if not DATA_PATH.exists():
        raise FileNotFoundError(
            f"Dataset not found at {DATA_PATH}. Download creditcard.csv and place it in the data folder."
        )

    print(f"Loading dataset from {DATA_PATH}")
    df = pd.read_csv(DATA_PATH)
    validate_dataset(df)

    X = df[FEATURE_COLUMNS]
    y = df[TARGET_COLUMN].astype(int)

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.20,
        random_state=42,
        stratify=y,
    )

    model = Pipeline(
        steps=[
            ("scaler", StandardScaler()),
            (
                "classifier",
                LogisticRegression(
                    max_iter=1000,
                    class_weight="balanced",
                    solver="lbfgs",
                    random_state=42,
                ),
            ),
        ]
    )

    print("Training Logistic Regression model...")
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    f1 = f1_score(y_test, y_pred, zero_division=0)
    cm = confusion_matrix(y_test, y_pred).tolist()
    report = classification_report(y_test, y_pred, zero_division=0)

    metrics = {
        "accuracy": float(accuracy),
        "f1_score": float(f1),
        "confusion_matrix": cm,
        "model": "LogisticRegression(class_weight='balanced')",
        "feature_columns": FEATURE_COLUMNS,
        "target_column": TARGET_COLUMN,
        "threshold": 0.5,
    }

    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(
        {
            "model": model,
            "feature_columns": FEATURE_COLUMNS,
            "target_column": TARGET_COLUMN,
            "threshold": 0.5,
            "metrics": metrics,
        },
        MODEL_PATH,
    )

    metrics_path = MODEL_PATH.parent / "metrics.json"
    metrics_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")

    print("\nModel performance")
    print(f"Accuracy: {accuracy:.6f}")
    print(f"F1-score: {f1:.6f}")
    print("\nClassification report")
    print(report)
    print(f"Model saved to {MODEL_PATH}")
    print(f"Metrics saved to {metrics_path}")


if __name__ == "__main__":
    main()
