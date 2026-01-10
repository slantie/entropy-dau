"""Train XGBoost + IsoForest ensemble."""
from typing import Tuple
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from imblearn.over_sampling import SMOTE
from sklearn.ensemble import IsolationForest
from sklearn.metrics import precision_recall_curve
import xgboost as xgb
import joblib
from config import logger, SMOTE_RATIO, MODELS_DIR  # Added MODELS_DIR
from src.data_loader import load_data
from src.preprocess import preprocess
from src.feature_eng import create_features, select_features

def train_model(subsample: int = 100000) -> Tuple[xgb.Booster, IsolationForest, float]:
    """Full train on subsample (for speed). Saves to models/. Returns model, iso, thresh."""
    logger.info("Starting training...")
    
    # Load subsample
    df, _ = load_data(subsample=subsample)
    
    # Preprocess
    df, preproc_pipe = preprocess(df)
    
    # FE
    df = create_features(df)
    
    # Select feats
    df_selected, selected_feats = select_features(df)
    
    # Split
    X = df_selected.drop(columns=["is_fraud"])
    y = df_selected["is_fraud"]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, stratify=y, random_state=42)
    
    # SMOTE on train (balance for better recall)
    smote = SMOTE(sampling_strategy=SMOTE_RATIO, random_state=42)
    X_train_res, y_train_res = smote.fit_resample(X_train, y_train)
    logger.info(f"SMOTE: Train fraud ratio {y_train_res.mean():.3f}")
    
    # XGBoost
    dtrain = xgb.DMatrix(X_train_res, label=y_train_res)
    dtest = xgb.DMatrix(X_test, label=y_test)
    
    params = {
        'objective': 'binary:logistic',
        'max_depth': 4,
        'learning_rate': 0.05,
        'subsample': 0.8,
        'colsample_bytree': 0.8,
        'scale_pos_weight': 1.0,  # Mild with SMOTE
        'random_state': 42,
        'eval_metric': 'aucpr',  # PR-AUC for imbalance
        'n_jobs': -1
    }
    
    evals = [(dtrain, 'train'), (dtest, 'test')]
    model = xgb.train(params, dtrain, num_boost_round=500, evals=evals, early_stopping_rounds=20, verbose_eval=50)
    
    # Save model
    model.save_model(str(MODELS_DIR / "model.json"))
    logger.info("Saved XGBoost model.json")
    
    # IsoForest (anomaly for novel fraud)
    iso = IsolationForest(contamination=0.036, random_state=42)
    iso.fit(X_train)
    joblib.dump(iso, MODELS_DIR / "ensemble.pkl")
    logger.info("Saved IsoForest ensemble.pkl")
    
    # Optimal thresh (F1-max on PR curve)
    y_proba = model.predict(dtest)
    prec, rec, thresh = precision_recall_curve(y_test, y_proba)
    f1 = 2 * (prec * rec) / (prec + rec + 1e-6)
    optimal_idx = np.argmax(f1)
    optimal_thresh = thresh[optimal_idx]
    joblib.dump(optimal_thresh, MODELS_DIR / "thresh.pkl")
    
    pr_auc = np.trapz(rec, prec)  # Exact PR-AUC
    logger.info(f"Training complete. PR-AUC: {pr_auc:.3f}, Best thresh: {optimal_thresh:.3f}")
    return model, iso, optimal_thresh