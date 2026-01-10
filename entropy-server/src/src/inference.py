"""Inference: Predict fraud probability for transactions."""
import pandas as pd
import numpy as np
from typing import List, Dict
import xgboost as xgb
import joblib
from pathlib import Path
from config import MODELS_DIR, logger

def predict_fraud_batch(transactions: List[Dict]) -> List[Dict]:
    """
    Predict fraud for a batch of transactions.
    
    Args:
        transactions: List of transaction dicts with features
    
    Returns:
        List of predictions with fraud_prob, is_fraud, confidence
    """
    try:
        # Load model & artifacts
        model_path = MODELS_DIR / "model.json"
        iso_path = MODELS_DIR / "ensemble.pkl"
        thresh_path = MODELS_DIR / "thresh.pkl"
        selected_feats_path = MODELS_DIR / "selected_features.pkl"
        
        if not model_path.exists():
            raise FileNotFoundError(f"Model not found at {model_path}. Run training first.")
        
        # Load model
        model = xgb.Booster()
        model.load_model(str(model_path))
        logger.info("Loaded XGBoost model")
        
        # Load artifacts
        iso = joblib.load(iso_path) if iso_path.exists() else None
        thresh = joblib.load(thresh_path) if thresh_path.exists() else 0.5
        selected_feats = joblib.load(selected_feats_path) if selected_feats_path.exists() else None
        
        # Convert to DataFrame
        df = pd.DataFrame(transactions)
        
        # Align features (fill missing with 0)
        if selected_feats:
            for feat in selected_feats:
                if feat not in df.columns:
                    df[feat] = 0
            X = df[selected_feats]
        else:
            X = df
        
        # Ensure numeric
        X = X.astype(float).fillna(0)
        
        # Predict
        dmatrix = xgb.DMatrix(X)
        probs = model.predict(dmatrix)
        
        # Add ensemble (IsoForest) if available
        if iso:
            iso_scores = iso.score_samples(X)
            # Normalize to [0, 1] and blend
            iso_probs = 1 / (1 + np.exp(iso_scores))  # Sigmoid
            probs = 0.8 * probs + 0.2 * iso_probs
        
        # Generate predictions
        predictions = []
        for prob in probs:
            is_fraud = int(prob > thresh)
            confidence = prob if is_fraud else (1 - prob)
            
            predictions.append({
                "fraud_probability": float(prob),
                "is_fraud": is_fraud,
                "confidence": float(confidence),
                "threshold_used": float(thresh)
            })
        
        logger.info(f"Predicted {len(predictions)} transactions")
        return predictions
    
    except Exception as e:
        logger.error(f"Inference error: {e}")
        raise


def predict_single(transaction: Dict) -> Dict:
    """Predict fraud for a single transaction."""
    return predict_fraud_batch([transaction])[0]
