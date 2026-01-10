"""Load & validate dataset."""
import pandas as pd
from pathlib import Path
from typing import Tuple
from config import DATA_PATH, logger

def load_data(subsample: int = None) -> Tuple[pd.DataFrame, int]:
    """Load CSV. Optional subsample for fast testing (e.g., 100k rows)."""
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"Dataset missing: {DATA_PATH}. Download to data/raw/")
    
    logger.info("Loading dataset...")
    df = pd.read_csv(DATA_PATH, low_memory=False)  # Handles large CSV
    
    # Subsample if specified (for quick tests)
    if subsample:
        df = df.sample(n=subsample, random_state=42).reset_index(drop=True)
        logger.info(f"Subsampled to {subsample:,} rows")
    
    # Validate core cols
    core_cols = ["is_fraud", "amount_ngn", "timestamp"]
    missing = [col for col in core_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Missing core cols: {missing}")
    
    fraud_rate = df["is_fraud"].mean()
    logger.info(f"Loaded: {df.shape[0]:,} rows, {df.shape[1]} cols | Fraud rate: {fraud_rate:.2%}")
    
    return df, df.shape[0]