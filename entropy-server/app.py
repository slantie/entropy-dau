import joblib
import numpy as np
import json
import pandas as pd
import sys
import os
from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from pathlib import Path
import logging
import pickle

# Patch pandas for legacy system compatibility (pandas 0.25.3)
try:
    if not hasattr(pd, 'StringDtype'):
        class StringDtype:
            """Stub StringDtype for legacy pandas versions"""
            pass
        pd.StringDtype = StringDtype
    
    if not hasattr(pd, 'BooleanDtype'):
        class BooleanDtype:
            """Stub BooleanDtype for legacy pandas versions"""
            pass
        pd.BooleanDtype = BooleanDtype
    
    if not hasattr(pd, 'Int64Dtype'):
        class Int64Dtype:
            """Stub Int64Dtype for legacy pandas versions"""
            pass
        pd.Int64Dtype = Int64Dtype
except:
    pass

# Now import xgboost after patching
try:
    import xgboost as xgb
except ImportError as e:
    print(f"Warning: XGBoost import failed: {e}")
    xgb = None

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Fix path to import service
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from src.feature_service import engineer_features

# Paths to the Kaggle XGB96 Ensemble artifacts
MODEL_BASE = Path("d:/Downloads/DAU Finale/ml/ieee-cis/saved_models")
XGB96_DIR = MODEL_BASE / "xgb96"
FEATURE_LIST_PATH = MODEL_BASE / "final_feature_list.pkl"

# Primary: Pickle ensemble path
PICKLE_MODEL_PATH = Path("d:/Downloads/DAU Finale/ml/xgb_ensemble_folds/xgb_ensemble_folds.pkl")

# Global ML State
MODELS = []
FEATURE_ORDER = []
THRESHOLD = 0.2  # Defined by user's evaluation code
ENCODING_MAPS = {}

def load_artifacts():
    global MODELS, FEATURE_ORDER, ENCODING_MAPS
    try:
        # Load the 6-fold XGBoost model ensemble from JSON files
        # (Pickle is incompatible with current XGBoost version)
        if xgb is not None:
            folks = sorted(list(XGB96_DIR.glob("*.json")))
            logger.info(f"Found {len(folks)} JSON model files in {XGB96_DIR}")
            
            for model_path in folks:
                try:
                    clf = xgb.Booster()
                    clf.load_model(str(model_path))
                    MODELS.append(clf)
                    logger.info(f"✅ Loaded {model_path.name}")
                except Exception as e:
                    logger.warning(f"Failed to load {model_path}: {e}")
        
        # 2. Load the exact 263 features used in training
        if FEATURE_LIST_PATH.exists():
            FEATURE_ORDER = joblib.load(FEATURE_LIST_PATH)
        
        # 3. Load Encoding Maps if available (FE, AG, etc.)
        maps_path = MODEL_BASE / "encoding_maps.json"
        if maps_path.exists():
            with open(maps_path, 'r') as f:
                ENCODING_MAPS = json.load(f)
        
        logger.info(f"✅ Loaded {len(MODELS)} models and {len(FEATURE_ORDER)} features.")
        if len(MODELS) == 0:
            logger.error("⚠️  WARNING: No models loaded! Predictions will fail.")
    except Exception as e:
        logger.error(f"❌ Error loading ML artifacts: {e}")
        import traceback
        traceback.print_exc()

load_artifacts()

app = FastAPI(title="Entropy XGB96 Simulation Server")

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    body = await request.body()
    # Log the body so we can see what's actually coming in
    logger.error(f"Validation Error Status 422")
    logger.error(f"Errors: {exc.errors()}")
    logger.error(f"Raw Body received: {body.decode()}")
    return JSONResponse(
        status_code=422,
        content={"detail": exc.errors(), "body": body.decode()},
    )

class SimulationRequest(BaseModel):
    transactionId: str
    raw: dict

@app.post("/predict")
def predict(req: SimulationRequest):
    logger.info(f"Received prediction request for TX: {req.transactionId}")
    try:
        if len(MODELS) == 0:
            raise HTTPException(status_code=503, detail="ML models not loaded")
        
        # 1. Engineering the "Magic" Features
        df_engineered = engineer_features(req.raw, encoding_maps=ENCODING_MAPS)
        
        # 2. Ensure feature parity
        X = df_engineered.reindex(columns=FEATURE_ORDER, fill_value=-1)
        
        # 3. Ensemble Prediction (Mean of 6 Folds)
        # MODELS contains XGBClassifier sklearn objects
        fold_probs = []
        for model in MODELS:
            # XGBClassifier.predict_proba returns array of shape (n_samples, n_classes)
            # For binary classification, we want the probability of class 1 (fraud)
            try:
                # Try predict_proba first (XGBClassifier method)
                if hasattr(model, 'predict_proba'):
                    probs = model.predict_proba(X)
                    prob = float(probs[0, 1]) if probs.shape[1] > 1 else float(probs[0, 0])
                else:
                    # Fallback to predict (if it's a Booster)
                    dmatrix = xgb.DMatrix(X)
                    prob = float(model.predict(dmatrix)[0])
                fold_probs.append(prob)
            except Exception as e:
                logger.warning(f"Model prediction failed: {e}")
                continue
            
        if not fold_probs:
            raise HTTPException(status_code=500, detail="All models failed to predict")
            
        avg_prob = float(np.mean(fold_probs))

        # 4. Classification
        status = "FRAUD" if avg_prob > THRESHOLD else "SAFE"
        confidence = avg_prob if status == "FRAUD" else (1 - avg_prob)

        # 5. Explainability (Feature Importance)
        try:
            # Get feature importance from first model
            if hasattr(MODELS[0], 'get_booster'):
                # XGBClassifier has get_booster() method
                booster = MODELS[0].get_booster()
                importance = booster.get_score(importance_type='gain')
            elif hasattr(MODELS[0], 'get_score'):
                # Direct Booster object
                importance = MODELS[0].get_score(importance_type='gain')
            else:
                importance = {}
        except Exception as e:
            logger.warning(f"Feature importance extraction failed: {e}")
            importance = {}
        top_features = []
        sorted_imp = sorted(importance.items(), key=lambda x: x[1], reverse=True)[:5]
        
        for feat_key, val in sorted_imp:
            top_features.append({
                "feature": feat_key,
                "value": float(X[feat_key].iloc[0]) if feat_key in X.columns else -1,
                "importance": round(float(val), 2)
            })

        return {
            "transactionId": req.transactionId,
            "riskScore": round(avg_prob, 4),
            "prediction": status,
            "confidence": round(confidence, 4),
            "threshold": THRESHOLD,
            "top_features": top_features,
            "stats": {
                "std": round(float(np.std(fold_probs)), 4),
                "folds": fold_probs
            }
        }
    except Exception as e:
        logger.error(f"Inference Error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
def health():
    return {
        "status": "active",
        "models_loaded": len(MODELS),
        "features_total": len(FEATURE_ORDER),
        "threshold": THRESHOLD
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

