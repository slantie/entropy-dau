"""
Integration Test Script for Entropy XGB96 Backend
Tests the full pipeline: Test Data -> Feature Engineering -> Ensemble Prediction -> Results Export
"""

import sys
import os
from pathlib import Path
import json
import numpy as np
import pandas as pd
import xgboost as xgb
import joblib

# Add src to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from src.feature_service import engineer_features

# Paths to artifacts
MODEL_BASE = Path("d:/Downloads/DAU Finale/ml/ieee-cis/saved_models")
XGB96_DIR = MODEL_BASE / "xgb96"
FEATURE_LIST_PATH = MODEL_BASE / "final_feature_list.pkl"
TEST_DATA_PATH = Path("d:/Downloads/DAU Finale/ml/ieee-cis/ieee-fraud-detection/test-data.xlsx")

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
    Run inference on a raw transaction (mirrors app.py logic)
    
    Returns:
        dict with predictions, confidence, and top features
    """
    try:
        # 1. Feature Engineering
        df_engineered = engineer_features(raw_transaction)
        
        # 2. Reindex to match training features
        X = df_engineered.reindex(columns=feature_order, fill_value=-1)
        
        # 3. Create DMatrix
        dmatrix = xgb.DMatrix(X)
        
        # 4. Ensemble Prediction (Mean of 6 Folds)
        fold_probs = []
        for model in models:
            prob = model.predict(dmatrix)[0]
            fold_probs.append(float(prob))
            
        avg_prob = float(np.mean(fold_probs))
        std_prob = float(np.std(fold_probs))
        
        # 5. Classification
        status = "FRAUD" if avg_prob > THRESHOLD else "SAFE"
        confidence = avg_prob if status == "FRAUD" else (1 - avg_prob)
        
        # 6. Explainability (Feature Importance)
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
        
        return {
            "transactionId": raw_transaction.get('TransactionID', 'UNKNOWN'),
            "riskScore": round(avg_prob, 4),
            "prediction": status,
            "confidence": round(confidence, 4),
            "threshold": THRESHOLD,
            "top_features": top_features,
            "stats": {
                "std": round(std_prob, 4),
                "folds": fold_probs
            },
            "success": True
        }
    except Exception as e:
        return {
            "transactionId": raw_transaction.get('TransactionID', 'UNKNOWN'),
            "error": str(e),
            "success": False
        }

def main():
    print("="*80)
    print("ENTROPY XGB96 INTEGRATION TEST")
    print("="*80)
    
    # Load models
    models, feature_order = load_models()
    
    # Load test data
    print(f"\n[DATA] Loading test data from: {TEST_DATA_PATH}")
    try:
        df_raw = pd.read_excel(TEST_DATA_PATH)
        print(f"✅ Loaded {len(df_raw)} transactions")
    except Exception as e:
        print(f"❌ Failed to load test data: {e}")
        return
    
    # Run inference on all transactions
    print(f"\n[PROCESSING] Running inference on {len(df_raw)} transactions...")
    print("="*80)
    
    results = []
    for idx, row in df_raw.iterrows():
        try:
            row_dict = row.to_dict()
            row_dict = {k: (v if pd.notna(v) else None) for k, v in row_dict.items()}
            
            result = run_inference(row_dict, models, feature_order)
            results.append(result)
            
            # Print progress every 50 transactions
            if (idx + 1) % 50 == 0:
                print(f"[{idx + 1}/{len(df_raw)}] Processed {idx + 1} transactions...")
            
        except Exception as e:
            print(f"[{idx + 1}] ❌ Error: {e}")
            results.append({
                "transactionId": row.get("TransactionID", idx),
                "error": str(e),
                "success": False
            })
    
    # Summary Report
    print("\n" + "="*80)
    print("INTEGRATION TEST RESULTS")
    print("="*80)
    
    successful = sum(1 for r in results if r.get("success", False))
    frauds = sum(1 for r in results if r.get("prediction") == "FRAUD")
    safes = sum(1 for r in results if r.get("prediction") == "SAFE")
    errors = sum(1 for r in results if not r.get("success", False))
    
    print(f"Total Transactions: {len(results)}")
    print(f"Successful: {successful}")
    print(f"Fraudulent: {frauds}")
    print(f"Safe: {safes}")
    print(f"Errors: {errors}")
    print(f"Fraud Rate: {(frauds / successful * 100):.2f}%" if successful > 0 else "N/A")
    
    # Export results to CSV
    output_csv = Path("d:/Downloads/DAU Finale/entropy-server/integration_test_results.csv")
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
    
    # Export summary statistics
    output_json = Path("d:/Downloads/DAU Finale/entropy-server/integration_test_summary.json")
    summary = {
        "total_transactions": len(results),
        "successful": successful,
        "fraudulent": frauds,
        "safe": safes,
        "errors": errors,
        "fraud_rate_percent": round((frauds / successful * 100), 2) if successful > 0 else 0,
        "threshold": THRESHOLD,
        "models_loaded": len(models),
        "features_total": len(feature_order)
    }
    with open(output_json, 'w') as f:
        json.dump(summary, f, indent=2)
    print(f"✅ Summary exported to: {output_json}")
    
    print("="*80)

if __name__ == "__main__":
    main()
