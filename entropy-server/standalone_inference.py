"""
Standalone Inference Script for IEEE-CIS Fraud Detection
Tests the full ML pipeline: raw transaction -> feature engineering -> ensemble prediction
Can be run directly without the FastAPI backend
"""

import sys
import os
from pathlib import Path
import json
import numpy as np
import pandas as pd
import xgboost as xgb
import joblib
import argparse

# Add src to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from src.feature_service import engineer_features

# Paths to artifacts
MODEL_BASE = Path("d:/Downloads/DAU Finale/ml/ieee-cis/saved_models")
XGB96_DIR = MODEL_BASE / "xgb96"
FEATURE_LIST_PATH = MODEL_BASE / "final_feature_list.pkl"

THRESHOLD = 0.2

def load_models():
    """Load the 6-fold XGBoost ensemble and feature order"""
    print("[LOADER] Loading models and features...")
    
    models = []
    folks = sorted(list(XGB96_DIR.glob("*.json")))
    for model_path in folks:
        clf = xgb.Booster()
        clf.load_model(str(model_path))
        models.append(clf)
    
    feature_order = []
    if FEATURE_LIST_PATH.exists():
        feature_order = joblib.load(FEATURE_LIST_PATH)
    
    print(f"✅ Loaded {len(models)} models")
    print(f"✅ Loaded {len(feature_order)} features")
    
    return models, feature_order

def run_inference(raw_transaction: dict, models: list, feature_order: list):
    """
    Run inference on a raw transaction
    
    Args:
        raw_transaction: dict with raw IEEE-CIS features
        models: list of XGBoost boosters
        feature_order: list of feature names in order
    
    Returns:
        dict with predictions, confidence, and top features
    """
    print(f"\n[INFERENCE] Processing transaction: {raw_transaction.get('id', 'UNKNOWN')}")
    print(f"[INFERENCE] Raw input keys: {list(raw_transaction.keys())}")
    
    # 1. Feature Engineering
    print("[ENGINEER] Running feature engineering...")
    try:
        df_engineered = engineer_features(raw_transaction)
        print(f"[ENGINEER] ✅ Engineered {len(df_engineered.columns)} features")
        print(f"[ENGINEER] Dtypes:\n{df_engineered.dtypes}")
    except Exception as e:
        print(f"[ENGINEER] ❌ Feature engineering failed: {e}")
        raise
    
    # 2. Reindex to match training features
    print("[REINDEX] Aligning to training feature set...")
    try:
        X = df_engineered.reindex(columns=feature_order, fill_value=-1)
        print(f"[REINDEX] ✅ Reindexed to {len(X.columns)} features")
        print(f"[REINDEX] Missing features (set to -1): {sum(X.isna().sum())}")
    except Exception as e:
        print(f"[REINDEX] ❌ Reindexing failed: {e}")
        raise
    
    # 3. Create DMatrix with categorical support
    print("[DMATRIX] Creating XGBoost DMatrix...")
    try:
        dmatrix = xgb.DMatrix(X, enable_categorical=True)
        print(f"[DMATRIX] ✅ DMatrix created successfully")
    except Exception as e:
        print(f"[DMATRIX] ❌ DMatrix creation failed: {e}")
        print(f"[DMATRIX] Column dtypes:\n{X.dtypes}")
        raise
    
    # 4. Ensemble Prediction
    print("[ENSEMBLE] Running predictions...")
    fold_probs = []
    for i, model in enumerate(models):
        try:
            prob = model.predict(dmatrix)[0]
            fold_probs.append(float(prob))
            print(f"  Fold {i}: {prob:.4f}")
        except Exception as e:
            print(f"[ENSEMBLE] ❌ Fold {i} prediction failed: {e}")
            raise
    
    avg_prob = float(np.mean(fold_probs))
    std_prob = float(np.std(fold_probs))
    
    # 5. Classification
    status = "FRAUD" if avg_prob > THRESHOLD else "SAFE"
    confidence = avg_prob if status == "FRAUD" else (1 - avg_prob)
    
    print(f"[ENSEMBLE] ✅ Average probability: {avg_prob:.4f}")
    print(f"[ENSEMBLE] Std Dev: {std_prob:.4f}")
    print(f"[ENSEMBLE] Classification: {status} (threshold: {THRESHOLD})")
    
    # 6. Explainability (Feature Importance)
    print("[EXPLAINABILITY] Computing top features...")
    importance = models[0].get_score(importance_type='gain')
    top_features = []
    sorted_imp = sorted(importance.items(), key=lambda x: x[1], reverse=True)[:5]
    
    for feat_key, val in sorted_imp:
        feat_val = float(X[feat_key].iloc[0]) if feat_key in X.columns else -1
        top_features.append({
            "feature": feat_key,
            "value": feat_val,
            "importance": round(float(val), 2)
        })
        print(f"  {feat_key}: importance={val:.2f}, value={feat_val}")
    
    return {
        "transactionId": raw_transaction.get('id', 'UNKNOWN'),
        "riskScore": round(avg_prob, 4),
        "prediction": status,
        "confidence": round(confidence, 4),
        "threshold": THRESHOLD,
        "top_features": top_features,
        "stats": {
            "std": round(std_prob, 4),
            "folds": fold_probs
        }
    }

def main():
    print("="*70)
    print("STANDALONE INFERENCE SCRIPT - IEEE-CIS Fraud Detection")
    print("="*70)
    
    # Parse arguments
    parser = argparse.ArgumentParser(description="Run inference on transactions from Excel file")
    parser.add_argument(
        "--file",
        type=str,
        default="d:/Downloads/DAU Finale/ml/ieee-cis/ieee-fraud-detection/test-data.xlsx",
        help="Path to Excel file with raw transactions"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of transactions to process"
    )
    args = parser.parse_args()
    
    # Load models
    models, feature_order = load_models()
    
    # Load transactions from Excel
    print(f"\n[DATA] Loading transactions from: {args.file}")
    try:
        df_raw = pd.read_excel(args.file)
        print(f"✅ Loaded {len(df_raw)} transactions")
        print(f"✅ Columns: {list(df_raw.columns)[:10]}...")  # Show first 10 columns
    except Exception as e:
        print(f"❌ Failed to load Excel file: {e}")
        return
    
    # Process transactions
    results = []
    limit = min(args.limit, len(df_raw))
    
    print(f"\n[PROCESSING] Running inference on {limit} transactions...")
    print("="*70)
    
    for idx in range(limit):
        try:
            row = df_raw.iloc[idx].to_dict()
            # Convert NaN to None for proper handling
            row = {k: (v if pd.notna(v) else None) for k, v in row.items()}
            
            result = run_inference(row, models, feature_order)
            results.append(result)
            
            # Print summary
            print(f"\n[{idx+1}/{limit}] TX {result['transactionId']}: "
                  f"{result['prediction']} (Risk: {result['riskScore']:.4f})")
            
        except Exception as e:
            print(f"\n[{idx+1}/{limit}] ❌ Error processing row {idx}: {e}")
            results.append({
                "transactionId": idx,
                "error": str(e)
            })
    
    # Summary Report
    print("\n" + "="*70)
    print("INFERENCE SUMMARY")
    print("="*70)
    
    frauds = sum(1 for r in results if r.get("prediction") == "FRAUD")
    safes = sum(1 for r in results if r.get("prediction") == "SAFE")
    errors = sum(1 for r in results if "error" in r)
    
    print(f"Total Processed: {len(results)}")
    print(f"Fraudulent: {frauds}")
    print(f"Safe: {safes}")
    print(f"Errors: {errors}")
    
    # Export results to CSV
    output_csv = Path("d:/Downloads/DAU Finale/entropy-server/inference_results.csv")
    results_df = pd.DataFrame([
        {
            "transactionId": r.get("transactionId", ""),
            "riskScore": r.get("riskScore", ""),
            "prediction": r.get("prediction", ""),
            "confidence": r.get("confidence", ""),
            "topFeatures": json.dumps(r.get("top_features", [])),
            "error": r.get("error", "")
        }
        for r in results
    ])
    results_df.to_csv(output_csv, index=False)
    print(f"\n✅ Results exported to: {output_csv}")
    print("="*70)

if __name__ == "__main__":
    main()
