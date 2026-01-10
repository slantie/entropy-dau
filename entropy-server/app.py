from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import joblib
import numpy as np
import xgboost as xgb
import json
import uvicorn
from pathlib import Path

MODEL_DIR = Path("model")
MODEL_PATH = MODEL_DIR / "model.json"
FEATURE_PATH = MODEL_DIR / "feature_order.json"
THRESH_PATH = MODEL_DIR / "thresh.pkl"

model = xgb.Booster()
model.load_model(str(MODEL_PATH))

with open(FEATURE_PATH, "r") as f:
    FEATURE_ORDER = json.load(f)

threshold = joblib.load(THRESH_PATH) if THRESH_PATH.exists() else 0.5

app = FastAPI(title="Fraud Detection ML Server")

class Transaction(BaseModel):
    transactionId: str
    features: dict

@app.post("/predict")
def predict(txn: Transaction):
    try:
        x = []
        for feature in FEATURE_ORDER:
            x.append(txn.features.get(feature, 0))

        X = np.array(x).reshape(1, -1)
        
        dmatrix = xgb.DMatrix(X, feature_names=FEATURE_ORDER)

        prob = float(model.predict(dmatrix)[0])

        if prob > threshold:
            status = "FRAUD"
            confidence = prob
        else:
            status = "SAFE"
            confidence = 1 - prob

        importance = model.get_score(importance_type='gain')
        
        top_features = []
        for feat, val in sorted(importance.items(), key=lambda x: x[1], reverse=True)[:5]:
            feat_idx = int(feat.replace('f', '')) if feat.startswith('f') else 0
            if feat_idx < len(FEATURE_ORDER):
                feat_name = FEATURE_ORDER[feat_idx]
                top_features.append({
                    "feature": feat_name,
                    "value": float(txn.features.get(feat_name, 0)),
                    "importance": float(val)
                })

        return {
            "transactionId": str(txn.transactionId),
            "riskScore": float(round(prob, 4)),
            "prediction": str(status),
            "confidence": float(round(confidence, 4)),
            "threshold": float(round(threshold, 4)),
            "top_features": top_features
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
def health():
    return {"status": "Entropy server is running"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
