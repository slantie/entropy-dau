"""Inference: Predict on new txns (for BE)."""
import xgboost as xgb
import joblib
import pandas as pd
import numpy as np
from typing import List, Dict, Tuple
from config import logger, OPTIMAL_THRESH, SELECTED_FEATS, MODELS_DIR
from src.preprocess import preprocess  # For pipe
from src.feature_eng import create_features  # For feats

# Load once at startup (prod: in BE __init__)
model = xgb.Booster(model_file=str(MODELS_DIR / "model.json"))
iso = joblib.load(MODELS_DIR / "ensemble.pkl")
selected_feats = joblib.load(MODELS_DIR / "selected_features.pkl")
thresh = joblib.load(MODELS_DIR / "thresh.pkl") or OPTIMAL_THRESH

def predict_fraud_batch(txns: List[Dict]) -> List[Dict]:
    """Batch predict. Input: List of txn dicts. Output: List with risk/action."""
    logger.info(f"Predicting on {len(txns)} txns")
    results = []
    
    # Preprocess batch
    df_batch = pd.DataFrame(txns)
    df_batch, _ = preprocess(df_batch)
    df_batch = create_features(df_batch)
    
    # Select feats (reindex to pad missing with 0)
    df_batch_selected = df_batch.reindex(columns=selected_feats, fill_value=0)
    
    # XGBoost proba
    dbatch = xgb.DMatrix(df_batch_selected)
    xgb_proba = model.predict(dbatch)
    
    # Anomaly score (novel fraud boost)
    anomaly_scores = -iso.decision_function(df_batch_selected)
    anomaly_norm = anomaly_scores / (anomaly_scores.max() + 1e-6)
    
    # Ensemble: 70% supervised + 30% unsupervised
    proba = 0.7 * xgb_proba + 0.3 * anomaly_norm
    
    for i, p in enumerate(proba):
        status = "Fraud" if p > thresh else "Legit"
        action = "Block" if p > 0.7 else "Review" if p > 0.3 else "Approve"
        explain = f"Risk {p:.2f} (anomaly {anomaly_norm[i]:.2f})"
        results.append({
            "risk_score": p,
            "status": status,
            "action": action,
            "explain": explain
        })
    
    logger.info(f"Predicted {len(results)} txns (avg risk {np.mean(proba):.3f})")
    return results