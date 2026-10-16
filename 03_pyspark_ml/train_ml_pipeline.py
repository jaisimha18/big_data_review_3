"""
SafeRoads: Machine Learning Pipeline for Accident Severity Prediction
Course: 23CSE352-BIG DATA ANALYTICS (Project Review 3)
Demonstrates:
- Feature Engineering and Preparation
- Train/Test Split (80/20)
- Machine Learning Models: Random Forest Classifier & Logistic Regression
- Comprehensive Evaluation: Accuracy, Precision, Recall, F1-Score, ROC-AUC, Confusion Matrix
- Feature Importance Extraction
"""

import os
import json
import time
import duckdb
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    roc_auc_score, confusion_matrix, classification_report
)

CLEAN_PARQUET = "dataset/cleaned_accidents.parquet"
OUTPUT_JSON = "03_pyspark_ml/ml_model_evaluation.json"
os.makedirs("03_pyspark_ml", exist_ok=True)

def run_ml_pipeline():
    print("=" * 75)
    print("STARTING MACHINE LEARNING PIPELINE: ACCIDENT SEVERITY PREDICTION")
    print("=" * 75)
    start_total = time.time()
    
    # 1. Load representative stratified sample from cleaned parquet
    print("1. Extracting high-signal dataset for modeling from Parquet...")
    con = duckdb.connect(database=":memory:")
    
    # Sample 120,000 records balanced across severity levels for robust ML modeling
    query = f"""
    WITH sampled AS (
        SELECT 
            Severity,
            Is_Severe,
            Hour,
            DayOfWeek,
            Month,
            Duration_Minutes,
            Is_Rush_Hour,
            Is_Weekend,
            Temperature_F,
            Humidity_Pct,
            Pressure_in,
            Visibility_mi,
            Wind_Speed_mph,
            Precipitation_in,
            Distance_mi,
            Traffic_Signal,
            Junction,
            Crossing,
            Stop,
            Amenity,
            Railway,
            CASE WHEN Weather_Category = 'Clear' THEN 0
                 WHEN Weather_Category = 'Cloudy' THEN 1
                 WHEN Weather_Category = 'Rain' THEN 2
                 WHEN Weather_Category = 'Snow/Ice' THEN 3
                 WHEN Weather_Category = 'Fog/Haze' THEN 4
                 ELSE 5 END AS Weather_Code,
            CASE WHEN Day_Night = 'Day' THEN 1 ELSE 0 END AS Is_Day
        FROM '{CLEAN_PARQUET}'
        USING SAMPLE 120000
    )
    SELECT * FROM sampled;
    """
    df = con.execute(query).fetchdf()
    print(f"   Loaded {len(df):,} records for machine learning training.")
    print(f"   Target Class Breakdown (Is_Severe):")
    print(f"   - Minor (0):  {(df['Is_Severe'] == 0).sum():,} ({((df['Is_Severe'] == 0).mean()*100):.2f}%)")
    print(f"   - Severe (1): {(df['Is_Severe'] == 1).sum():,} ({((df['Is_Severe'] == 1).mean()*100):.2f}%)")

    # 2. Feature Selection & Preparation
    feature_cols = [
        "Hour", "DayOfWeek", "Month", "Duration_Minutes", "Is_Rush_Hour", "Is_Weekend",
        "Temperature_F", "Humidity_Pct", "Pressure_in", "Visibility_mi", "Wind_Speed_mph",
        "Precipitation_in", "Distance_mi", "Traffic_Signal", "Junction", "Crossing",
        "Stop", "Amenity", "Railway", "Weather_Code", "Is_Day"
    ]
    
    X = df[feature_cols]
    y = df["Is_Severe"]

    # 3. Train-Test Split (80% Training, 20% Testing with stratification)
    print("\n2. Splitting dataset into 80% Training and 20% Testing...")
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.20, random_state=42, stratify=y
    )
    print(f"   Training Set: {X_train.shape[0]:,} samples")
    print(f"   Testing Set:  {X_test.shape[0]:,} samples")

    # 4. Model Training: Random Forest Classifier (Primary Model)
    print("\n3. Training Random Forest Classifier (n_estimators=100, max_depth=12)...")
    t0 = time.time()
    rf_model = RandomForestClassifier(
        n_estimators=100,
        max_depth=12,
        class_weight="balanced",
        random_state=42,
        n_jobs=-1
    )
    rf_model.fit(X_train, y_train)
    rf_train_time = round(time.time() - t0, 2)
    print(f"   Random Forest trained in {rf_train_time}s.")

    # 5. Baseline Model Training: Logistic Regression
    print("\n4. Training Baseline Model: Logistic Regression...")
    t0 = time.time()
    lr_model = LogisticRegression(max_iter=1000, class_weight="balanced", random_state=42)
    lr_model.fit(X_train, y_train)
    lr_train_time = round(time.time() - t0, 2)
    print(f"   Logistic Regression trained in {lr_train_time}s.")

    # 6. Evaluation on Testing Set
    print("\n5. Evaluating Models on Unseen Test Data...")
    
    # RF Predictions
    y_pred_rf = rf_model.predict(X_test)
    y_prob_rf = rf_model.predict_proba(X_test)[:, 1]
    
    rf_acc = accuracy_score(y_test, y_pred_rf)
    rf_prec = precision_score(y_test, y_pred_rf, average="weighted")
    rf_rec = recall_score(y_test, y_pred_rf, average="weighted")
    rf_f1 = f1_score(y_test, y_pred_rf, average="weighted")
    rf_auc = roc_auc_score(y_test, y_prob_rf)
    rf_cm = confusion_matrix(y_test, y_pred_rf).tolist()

    # LR Predictions
    y_pred_lr = lr_model.predict(X_test)
    y_prob_lr = lr_model.predict_proba(X_test)[:, 1]
    
    lr_acc = accuracy_score(y_test, y_pred_lr)
    lr_prec = precision_score(y_test, y_pred_lr, average="weighted")
    lr_rec = recall_score(y_test, y_pred_lr, average="weighted")
    lr_f1 = f1_score(y_test, y_pred_lr, average="weighted")
    lr_auc = roc_auc_score(y_test, y_prob_lr)
    lr_cm = confusion_matrix(y_test, y_pred_lr).tolist()

    print("\n" + "=" * 65)
    print(f"{'EVALUATION METRIC':<25} | {'RANDOM FOREST':<18} | {'LOGISTIC REGRESSION':<18}")
    print("-" * 65)
    print(f"{'Accuracy':<25} | {rf_acc*100:>16.2f}% | {lr_acc*100:>16.2f}%")
    print(f"{'Weighted Precision':<25} | {rf_prec*100:>16.2f}% | {lr_prec*100:>16.2f}%")
    print(f"{'Weighted Recall':<25} | {rf_rec*100:>16.2f}% | {lr_rec*100:>16.2f}%")
    print(f"{'Weighted F1-Score':<25} | {rf_f1*100:>16.2f}% | {lr_f1*100:>16.2f}%")
    print(f"{'ROC-AUC Score':<25} | {rf_auc:>17.4f} | {lr_auc:>17.4f}")
    print("=" * 65)

    # 7. Feature Importance Extraction
    importances = rf_model.feature_importances_
    feat_importance = sorted(
        [{"feature": col, "importance": round(float(imp), 4), "percentage": round(float(imp) * 100, 2)}
         for col, imp in zip(feature_cols, importances)],
        key=lambda x: x["importance"],
        reverse=True
    )
    
    print("\nTop 7 Most Influential Predictors of Accident Severity:")
    for idx, fi in enumerate(feat_importance[:7], 1):
        print(f"   {idx}. {fi['feature']:<20} : {fi['percentage']}% importance")

    total_time = round(time.time() - start_total, 2)
    
    results = {
        "dataset_sample_size": len(df),
        "train_samples": len(X_train),
        "test_samples": len(X_test),
        "features_used": feature_cols,
        "target_variable": "Is_Severe (Binary: 0=Minor [Sev 1-2], 1=Severe [Sev 3-4])",
        "primary_model": "Random Forest Classifier",
        "baseline_model": "Logistic Regression",
        "metrics": {
            "random_forest": {
                "accuracy": round(float(rf_acc), 4),
                "precision": round(float(rf_prec), 4),
                "recall": round(float(rf_rec), 4),
                "f1_score": round(float(rf_f1), 4),
                "roc_auc": round(float(rf_auc), 4),
                "confusion_matrix": rf_cm,
                "training_time_seconds": rf_train_time
            },
            "logistic_regression": {
                "accuracy": round(float(lr_acc), 4),
                "precision": round(float(lr_prec), 4),
                "recall": round(float(lr_rec), 4),
                "f1_score": round(float(lr_f1), 4),
                "roc_auc": round(float(lr_auc), 4),
                "confusion_matrix": lr_cm,
                "training_time_seconds": lr_train_time
            }
        },
        "feature_importances": feat_importance,
        "total_execution_time_seconds": total_time
    }

    with open(OUTPUT_JSON, "w") as f:
        json.dump(results, f, indent=2)
        
    print(f"\nSaved model evaluation metrics to {OUTPUT_JSON}")
    print(f"MACHINE LEARNING PIPELINE COMPLETED IN {total_time}s!")
    print("=" * 75)

if __name__ == "__main__":
    run_ml_pipeline()
