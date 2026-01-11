import pandas as pd
import numpy as np
import datetime

START_DATE = datetime.datetime.strptime('2017-11-30', '%Y-%m-%d')

SELECTED_V = [
    1, 3, 4, 6, 8, 11, 13, 14, 17, 20, 23, 26, 27, 30, 36, 37, 40, 41, 44, 47, 48,
    54, 56, 59, 62, 65, 67, 68, 70, 76, 78, 80, 82, 86, 88, 89, 91, 107, 108, 111,
    115, 117, 120, 121, 123, 124, 127, 129, 130, 136, 138, 139, 142, 147, 156, 160,
    162, 165, 166, 169, 171, 173, 175, 176, 178, 180, 182, 185, 187, 188, 198, 203,
    205, 207, 209, 210, 215, 218, 220, 221, 223, 224, 226, 228, 229, 234, 235, 238,
    240, 250, 252, 253, 257, 258, 260, 261, 264, 266, 267, 271, 274, 277, 281, 283,
    284, 285, 286, 289, 291, 294, 296, 297, 301, 303, 305, 307, 309, 310, 314, 320
]

CAT_COLS = [
    'ProductCD', 'card4', 'card6', 'P_emaildomain', 'R_emaildomain',
    'M1', 'M2', 'M3', 'M4', 'M5', 'M6', 'M7', 'M8', 'M9',
    'id_12', 'id_15', 'id_16', 'id_23', 'id_27', 'id_28', 'id_29', 'id_30',
    'id_31', 'id_33', 'id_34', 'id_35', 'id_36', 'id_37', 'id_38', 'DeviceType', 'DeviceInfo'
]

def engineer_features(raw_data: dict, encoding_maps: dict = None):
    """
    Implements the XGB96 "Magic" Feature Engineering.
    Expects raw_data to be a flat dict of IEEE-CIS features.
    """
    df = pd.DataFrame([raw_data])
    
    # 1. Expand identity and vFeatures if nested (from Prisma)
    # We drop them immediately to avoid duplicates before joining flattened data
    if 'identity' in df.columns and isinstance(df['identity'].iloc[0], dict):
        id_df = pd.json_normalize(df['identity'])
        df = pd.concat([df.drop(columns=['identity']), id_df], axis=1)
    if 'vFeatures' in df.columns and isinstance(df['vFeatures'].iloc[0], dict):
        v_df = pd.json_normalize(df['vFeatures'])
        df = pd.concat([df.drop(columns=['vFeatures']), v_df], axis=1)

    # CRITICAL: Remove any duplicate columns that might have appeared from joins/expansions
    # Also copies to avoid fragmentation warnings
    df = df.loc[:, ~df.columns.duplicated()].copy()

    # 2. Basic Cleaning / Normalization (Mirroring Notebook #VSC-930277f2)
    # Ensure numeric types for math operations
    numeric_cols = ['TransactionAmt', 'TransactionDT', 'D1', 'D15', 'addr1', 'card1', 'dist1']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(-1)

    # 3. Normalize D Columns (Mirroring Notebook #VSC-09484ddc)
    # We use float32 to match training precision
    for i in range(1, 16):
        if i in [1, 2, 3, 5, 9]: continue
        d_col = f'D{i}'
        if d_col in df.columns:
            df[d_col] = df[d_col] - df['TransactionDT'] / np.float32(24*60*60)

    # 4. Create Derived Features (Mirroring Notebook #VSC-01556714)
    df['day'] = df['TransactionDT'] / (24*60*60)
    df['cents'] = (df['TransactionAmt'] - np.floor(df['TransactionAmt'])).astype('float32')
    
    # UID Creation (The "Magic")
    # Note: card1_addr1 must be created first
    df['card1_addr1'] = df['card1'].astype(str) + '_' + df['addr1'].astype(str)
    df['card1_addr1_P_emaildomain'] = df['card1_addr1'].astype(str) + '_' + df['P_emaildomain'].astype(str)
    
    # Magic UID
    # uid = card1_addr1 + np.floor(day - D1)
    df['uid'] = df['card1_addr1'].astype(str) + '_' + np.floor(df['day'] - df['D1'].astype(float)).astype(str)
    
    # 5. Complex Aggregations / Encodings
    # In production inference, these are LOOKUPS from training statistics.
    # If encoding_maps is provided, we map them. 
    # Otherwise, we use placeholders or -1.
    
    if encoding_maps:
        for feat, mapping in encoding_maps.items():
            if feat.endswith('_FE'):
                orig = feat.replace('_FE', '')
                if orig in df.columns:
                    df[feat] = df[orig].map(mapping).fillna(-1).astype('float32')
            elif '_mean' in feat or '_std' in feat or '_ct' in feat:
                # These are usually grouped by uid, card1, etc.
                # In single-transaction inference, we look up the group stats.
                parts = feat.split('_')
                group_col = 'uid' if 'uid' in parts else 'card1' # heuristics
                if group_col in df.columns:
                    group_val = str(df[group_col].iloc[0])
                    df[feat] = mapping.get(group_val, -1)

    # 6. Final Clean up (Outsider15)
    df['outsider15'] = (np.abs(df['D1'] - df['D15']) > 3).astype('int8')

    # 7. Categorical Handling for XGBoost
    # Convert categorical columns to numeric codes instead of category dtype
    # This avoids XGBoost's vectorize error on empty categories
    for col in df.columns:
        if df[col].dtype.name == 'category':
            # Convert to numeric codes
            df[col] = df[col].cat.codes.astype('int64')
        elif df[col].dtype == 'object':
            # Try to convert to numeric, else use -1
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(-1).astype('int64')
        elif col not in numeric_cols and col not in ['uid', 'TransactionID', 'id']:
            # Ensure other columns are numeric if possible
            try:
                if not pd.api.types.is_numeric_dtype(df[col]):
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(-1)
            except:
                pass

    # FINAL PARANOIA: Ensure no duplicate columns before returning to app.py
    # Reindexing fails on duplicate labels.
    df = df.loc[:, ~df.columns.duplicated()].copy()
    
    return df
