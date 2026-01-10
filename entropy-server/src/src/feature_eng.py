"""Feature Engineering: Add anomalies, encode cats."""
from typing import List, Tuple
import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder
from sklearn.ensemble import RandomForestClassifier
import joblib
from config import N_TOP_FEATS, MODELS_DIR, logger
import warnings
warnings.filterwarnings('ignore')

def create_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add behavioral/anomaly feats, encode categoricals."""
    logger.info("Engineering features...")
    
    # 1. Behavioral z-scores (deviation from user norms)
    if all(col in df for col in ["amount_ngn", "user_avg_txn_amt", "user_std_txn_amt"]):
        df["amount_zscore"] = (df["amount_ngn"] - df["user_avg_txn_amt"]) / (df["user_std_txn_amt"] + 1e-6)
        logger.info("Added amount_zscore")
    
    # 2. Velocity anomaly (high txn rate)
    if all(col in df for col in ["txn_count_last_1h", "user_txn_frequency_24h"]):
        df["velocity_anomaly"] = df["txn_count_last_1h"] / (df["user_txn_frequency_24h"] + 1)
        logger.info("Added velocity_anomaly")
    
    # 3. Composite anomaly (sum of key signals)
    anomaly_cols = ["spending_deviation_score", "geo_anomaly_score", "velocity_score"]
    avail = [c for c in anomaly_cols if c in df.columns]
    if avail:
        df["composite_anomaly"] = df[avail].sum(axis=1) / len(avail)
        logger.info("Added composite_anomaly")
    
    # 4. Risk interaction (channel risk * night txn)
    if all(col in df for col in ["channel_risk_score", "is_night_txn"]):
        df["risk_night_interaction"] = df["channel_risk_score"] * df["is_night_txn"]
        logger.info("Added risk_night_interaction")
    
    # 5. Sharing risk (device + IP shared)
    sharing_cols = ["is_device_shared", "is_ip_shared"]
    avail = [c for c in sharing_cols if c in df.columns]
    if avail:
        df["sharing_risk"] = df[avail].sum(axis=1)
        logger.info("Added sharing_risk")
    
    # 6. Encode categoricals (one-hot low-card, label high-card)
    cat_low = ["payment_channel", "merchant_category", "sender_persona", "transaction_type", "user_top_category"]
    low_avail = [c for c in cat_low if c in df.columns]
    if low_avail:
        df = pd.get_dummies(df, columns=low_avail, drop_first=True, dtype=int)
        logger.info(f"One-hot encoded {len(low_avail)} low-card cats")
    
    cat_high = ["location", "device_used", "ip_geo_region"]
    high_avail = [c for c in cat_high if c in df.columns]
    for col in high_avail:
        le = LabelEncoder()
        df[col] = le.fit_transform(df[col].astype(str))
        logger.info(f"Label encoded {col}")
    
    new_feats = [c for c in df.columns if 'zscore' in c or 'anomaly' in c or 'interaction' in c or 'sharing' in c]
    logger.info(f"Added {len(new_feats)} new feats. New shape: {df.shape}")
    return df

def select_features(df: pd.DataFrame, target_col: str = "is_fraud") -> Tuple[pd.DataFrame, List[str]]:
    """Select top N feats using RF importance (subsample for speed)."""
    logger.info("Selecting features...")
    
    X = df.drop(columns=[target_col])
    y = df[target_col]
    
    # Subsample for speed (100k max)
    sample_size = min(100000, len(X))
    if sample_size < len(X):
        sample_idx = np.random.choice(len(X), sample_size, replace=False)
        X_sample = X.iloc[sample_idx].reset_index(drop=True)
        y_sample = y.iloc[sample_idx].reset_index(drop=True)
        logger.info(f"Subsampled {sample_size:,} for importance")
    else:
        X_sample, y_sample = X, y
    
    # Ensure all numeric (label encode remaining objects)
    object_cols = X_sample.select_dtypes(include=['object']).columns
    if len(object_cols) > 0:
        for col in object_cols:
            le = LabelEncoder()
            X_sample[col] = le.fit_transform(X_sample[col].astype(str))
        logger.info(f"Label encoded {len(object_cols)} remaining object cols")
    
    # Force all to float (fixes dtype ambiguity/NaN issues)
    X_sample = X_sample.astype(float).fillna(0)
    
    # RF importance
    rf = RandomForestClassifier(n_estimators=50, random_state=42, n_jobs=-1)
    rf.fit(X_sample, y_sample)
    
    importance = pd.Series(rf.feature_importances_, index=X.columns).sort_values(ascending=False)
    top_feats = importance.head(N_TOP_FEATS).index.tolist()
    
    df_selected = df[top_feats + [target_col]].copy()
    
    # Save
    joblib.dump(top_feats, MODELS_DIR / "selected_features.pkl")
    
    logger.info(f"Selected top {N_TOP_FEATS} feats (e.g., {top_feats[:5]})")
    return df_selected, top_feats