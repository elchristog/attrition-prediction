import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.feature_selection import SelectFromModel
import logging

logger = logging.getLogger(__name__)

class AttritionFeatureEngineer:
    def __init__(self, horizon="8M", attrition_typr="HARD"):
        self.label_encoders = {}
        self.feature_cols = []
        self.horizon = horizon.upper()
        self.attrition_type = attrition_typr.upper()
        if self.attrition_type in ["HARD", "SILENT"]:
            self.target_col = f"TARGET_{self.attrition_type}_{self.horizon}"
        else:
            self.target_col = f"TARGET_{self.horizon}"
        
    def preprocess_data(self, df, is_training=True):
        """
        Cleans and prepends features for the model.
        """
        # 0. Ensure target is integer
        if self.target_col in df.columns:
            df[self.target_col] = df[self.target_col].fillna(0).astype(int)

        # 1. Drop non-features, IDs, and other potential leakage targets/flags
        # Drop current month attrition status and all exclusionary metadata
        drop_cols = ["ORG_URI", "COHORT_MONTH", "HISTORICAL_ATTRITION_DECISION", 
                     "FLIP_DATE", "ATTRITION_DATE", "TARGET_CURRENT_MONTH"]
        
        # Identify all other TARGET_* and EXCL_FLAG_* columns to prevent leakage
        other_horizons = [col for col in df.columns if (col.startswith("TARGET_") or col.startswith("EXCL_FLAG_")) 
                          and col != self.target_col]
        drop_cols.extend(other_horizons)
        
        # Shubhi's recommendations for Seasonality and Behavioral signals
        base_features = [
            'SPEND_L3M_VS_BLENDED_RATIO',
            # 'CONSECUTIVE_INACTIVE_MONTHS',
            'DECLINED_TXN_RATE_L6M',
            'ACTIVE_MONTHS_RATE_L12M',
            'FEE_TO_REVENUE_RATIO_MTH',
            'LATE_FEE_TO_TOTAL_FEE_RATIO',
            'ENT_GALLONS_VELOCITY_3V12',
            'IS_TRUCKING_INDUSTRY',
            'ACCOUNT_COUNT',
            'ENT_CASE_TOTAL_LAG2',
            'ENT_FEES_LAG1'
        ]

        # Modify these lists to define distinct feature sets for each model type
        if self.attrition_type == "HARD":
            selected_features = [
            'SPEND_L3M_VS_BLENDED_RATIO',
            'DECLINED_TXN_RATE_L6M',
            'DECLINED_TXN_RATE_MTH',           # current-month spike: immediate driver friction
            'ACTIVE_MONTHS_RATE_L12M',
            'FEE_TO_REVENUE_RATIO_MTH',
            'FEE_RATIO_TREND_3M',
            'ENT_GALLONS_VELOCITY_3V12',
            'IS_TRUCKING_INDUSTRY',
            'ACCOUNT_COUNT',
            'ENT_CASE_TOTAL_LAG2',
            'ENT_CASE_TREND_3M',
            'ENT_CASE_PER_1K_GALLONS',
            'MONTHS_SINCE_CS_TERMINATION_CASE', # recency of formal termination case in CRM
            'ENT_FEES_LAG1',
            'CURRENT_VOLUME_VS_PEAK_PCT',
            'IS_SMALL_BIZ'
        ]
        elif self.attrition_type == "SILENT":
            selected_features = [
            'SPEND_L3M_VS_BLENDED_RATIO',
            'DECLINED_TXN_RATE_L6M',
            'DECLINED_TXN_RATE_MTH',           # current-month spike: immediate driver friction
            'ACTIVE_MONTHS_RATE_L12M',
            'FEE_TO_REVENUE_RATIO_MTH_LAG1',
            'FEE_RATIO_TREND_3M',
            'ENT_GALLONS_VELOCITY_3V12',
            'ENT_GALLONS_VELOCITY_YOY',
            'IS_TRUCKING_INDUSTRY',
            'ACCOUNT_COUNT',
            'ENT_CASE_TOTAL_LAG2',
            'ENT_CASE_TREND_3M',
            'ENT_CASE_PER_1K_GALLONS',
            'MONTHS_SINCE_CS_TERMINATION_CASE', # recency of formal termination case in CRM
            'ENT_FEES_LAG1',
            'HISTORICAL_MAX_DROP_PCT',
            'CURRENT_VOLUME_VS_PEAK_PCT',
            'IS_SMALL_BIZ',
            'ENT_GALLONS_AVG_3M'
        ]
        else:  # General Attrition
            selected_features = base_features.copy()  # Start with base features for general attrition
        
        # Apply history exclusion flags only during training.
        # During inference we score all active accounts regardless of history length —
        # immature features get median-imputed below. Excluding them at scoring time
        # would silently drop new accounts that may be at real attrition risk.
        if is_training and 'EXCL_FLAG_HIST_15M' in df.columns:
            df = df[df['EXCL_FLAG_HIST_15M'] == 0]
            logger.info(f"Filtered by EXCL_FLAG_HIST_15M. Remaining rows: {len(df)}")
        # We also need to extract the target column before subsetting
        if is_training:
            self.target = df[self.target_col]
            # Robust label encoding for target
            self.target_le = LabelEncoder()
            self.target = self.target_le.fit_transform(self.target)
            
        features = df[[col for col in selected_features if col in df.columns]].copy()
        
        # 2. Handle Categorical Columns
        categorical_cols = features.select_dtypes(include=["object"]).columns
        for col in categorical_cols:
            if is_training:
                le = LabelEncoder()
                features[col] = le.fit_transform(features[col].astype(str))
                self.label_encoders[col] = le
            else:
                le = self.label_encoders.get(col)
                if le:
                    # Handle unseen labels by mapping them to the most frequent or a default
                    features[col] = features[col].apply(lambda x: le.transform([x])[0] if x in le.classes_ else -1)

        # 3. Handle Missing Values
        # Fill numeric with median, categorical (already encoded) with -1
        numeric_cols = features.select_dtypes(include=[np.number]).columns
        features[numeric_cols] = features[numeric_cols].fillna(features[numeric_cols].median())
        
        if is_training:
            self.feature_cols = features.columns.tolist()
            return features, self.target
        else:
            return features[self.feature_cols]


if __name__ == "__main__":
    # Example usage (Mock data)
    fe = AttritionFeatureEngineer(horizon="8M", attrition_typr="HARD")
    # Mock DF
    data = pd.DataFrame({
        "ORG_URI": ["A", "B"],
        "ENT_GALLONS": [100, 200],
        "PARTNER_IND": ["Wex", "Partner"],
        "TARGET_8M": [0, 1],
        "TARGET_HARD_8M": [0, 1],
    })
    X, y = fe.preprocess_data(data)
    print(X.head())
