"""Preprocessing: Handle missing, scale, extract datetime."""
from typing import Tuple
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
import joblib
from config import MODELS_DIR, logger

def preprocess(df: pd.DataFrame) -> Tuple[pd.DataFrame, object]:
    """Clean data, extract feats, fit pipeline. Returns processed DF + fitted pipe."""
    logger.info("Starting preprocessing...")
    
    # 1. Datetime extract (if timestamp)
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df["txn_hour"] = df["timestamp"].dt.hour.fillna(0)
        df["txn_dayofweek"] = df["timestamp"].dt.dayofweek.fillna(0)
        df = df.drop(columns=["timestamp"])  # Drop original after extract
        logger.info("Extracted temporal feats & dropped timestamp")
    
    # 2. Bool to int (for ML)
    bool_cols = df.select_dtypes(include=bool).columns
    for col in bool_cols:
        df[col] = df[col].astype(int)
    logger.info(f"Converted {len(bool_cols)} bool cols to int")
    
    # 3. Impute + Scale pipeline (numeric only, exclude target)
    num_cols = df.select_dtypes(include="number").columns
    target_col = "is_fraud"
    if target_col in num_cols:
        num_cols = num_cols.drop(target_col)
    
    if len(num_cols) > 0:
        pipe = Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler())
        ])
        df[num_cols] = pipe.fit_transform(df[num_cols])
        joblib.dump(pipe, MODELS_DIR / "preprocessor.pkl")
        logger.info(f"Processed {len(num_cols)} numeric cols")
    else:
        pipe = None
        logger.warning("No numeric cols found!")
    
    # 4. Drop ID/sensitive + fraud_type (leak)
    drop_cols = ["transaction_id", "sender_account", "ip_address", "device_hash", "fraud_type"]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns])
    logger.info(f"Dropped {len([c for c in drop_cols if c in df.columns])} ID/sensitive/leak cols")
    
    logger.info("Preprocessing complete")
    return df, pipe