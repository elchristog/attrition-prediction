import os
import pathlib
import pandas as pd
import numpy as np
import logging
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import average_precision_score, precision_recall_curve
from sklearn.feature_selection import mutual_info_classif
from scipy.stats import chi2_contingency

# Windows compatibility patch for Snowpark OSError [WinError 123]
_orig_is_file = pathlib.Path.is_file
def _patched_is_file(self):
    try:
        if "<" in str(self) or ">" in str(self): return False
        return _orig_is_file(self)
    except Exception: return False
pathlib.Path.is_file = _patched_is_file

os.environ["SNOWPARK_SKIP_SOURCE_CODE_COLLECTION"] = "True"

logger = logging.getLogger(__name__)

class MultiHorizonProfiler:
    def __init__(self, targets=("TARGET_HARD_8M", "TARGET_SILENT_8M")):
        self.targets = [t.upper() for t in targets]
        self.metrics_store = []
        
        # Approved Base Metrics (Predictive Telemetry)
        self.approved_base = [
            'GROSS_SPEND', 'FUEL_SPEND', 'GALLONS', 'REVENUE',
            'TRANSACTION_COUNT', 'DECLINED_TXN', 'SR_COUNT', 'SR_CLOSED',
            'LATE_FEE', 'SERVFEE', 'DELIVERY_FEE', 'TOTAL_FEE', 'REBATE', 'FEES',
            'MAX_WX_DPD', 'TOTAL_EXPOSURE', 'EXPOSURE', 'MAX_CR_LIMIT', 'CREDIT_LIMIT', 
            'AVG_OUTSTANDING_BALANCE', 'BALANCE',
            'INN_SPEND', 'OON_SPEND', 'INN_GALLONS', 'OON_GALLONS',
            'GALLONS_VELOCITY_YOY', 'HISTORICALLY_SEASONAL_FLAG', 
            'HISTORICAL_ZERO_GALLON_MONTHS', 'HISTORICAL_MAX_DROP_PCT',
            'SPEND_L3M_VS_BLENDED_RATIO', 'CONSECUTIVE_INACTIVE_MONTHS',
            'DECLINED_TXN_RATE_L6M', 'ACTIVE_MONTHS_RATE_L12M',
            'FEE_TO_REVENUE_RATIO_MTH', 'LATE_FEE_TO_TOTAL_FEE_RATIO'
        ]
        
        # Approved Firmographics (Static/Profile)
        self.approved_profile = [
            'IS_SMALL_BIZ', 'IS_TRUCKING_INDUSTRY', 'HAS_VIP_ACCOUNT', 'HAS_GOVT_ACCOUNT',
            'AVG_TENURE_MONTHS', 'MAX_TENURE_MONTHS', 'ACCOUNT_COUNT', 'ACTIVE_ACCOUNT_COUNT',
            'MAX_RISK_GRADE', 'AVG_ACQUISITION_FICO'
        ]

    def _get_intelligent_feature_list(self, available_columns):
        """
        Filters columns to include only predictive features and their rolling windows.
        """
        feature_list = []
        cols_upper = [c.upper() for c in available_columns]
        
        # 1. Add Profile features
        for f in self.approved_profile:
            if f in cols_upper:
                # Find original case
                feature_list.append(available_columns[cols_upper.index(f)])
                
        # 2. Add Base metrics and all their rolling variations (SUM, AVG, L1M etc)
        for base in self.approved_base:
            for col, col_up in zip(available_columns, cols_upper):
                # Match "GROSS_SPEND", "ENT_GROSS_SPEND", "ENT_GROSS_SPEND_SUM_L3M" etc
                is_match = (
                    col_up == base or 
                    col_up.startswith(f"{base}_") or 
                    col_up == f"ENT_{base}" or 
                    col_up.startswith(f"ENT_{base}_")
                )
                if is_match and col not in feature_list:
                    feature_list.append(col)
                        
        # 3. Add "Case" friction signals
        for col, col_up in zip(available_columns, cols_upper):
            # Matches CASE_..._COUNT or ENT_CASE_...
            if (col_up.startswith("CASE_") and col_up.endswith("_COUNT")) or col_up.startswith("ENT_CASE_"):
                if col not in feature_list:
                    feature_list.append(col)
                    
        return feature_list


    def calculate_iv_psi(self, train_df, test_df, feature, target):
        """
        Calculates Information Value (IV) and Population Stability Index (PSI).
        """
        # 1. Bining (Deciles)
        try:
            train_df['bin'] = pd.qcut(train_df[feature].rank(method='first'), 10, labels=False, duplicates='drop')
        except:
            train_df['bin'] = 0 # Fallback for low cardinality
            
        # 2. IV Calculation
        stats = train_df.groupby('bin').agg(
            events=(target, 'sum'),
            total=(target, 'count')
        )
        stats['non_events'] = stats['total'] - stats['events']
        
        total_events = stats['events'].sum()
        total_non_events = stats['non_events'].sum()
        
        # Avoid division by zero
        stats['dist_events'] = stats['events'] / (total_events if total_events > 0 else 1)
        stats['dist_non_events'] = stats['non_events'] / (total_non_events if total_non_events > 0 else 1)
        
        # WOE = log(non_event_dist / event_dist)
        stats['woe'] = np.log((stats['dist_non_events'] + 0.001) / (stats['dist_events'] + 0.001))
        stats['iv'] = (stats['dist_non_events'] - stats['dist_events']) * stats['woe']
        total_iv = stats['iv'].sum()
        
        # 3. PSI Calculation (Train vs Test distribution)
        if test_df is not None and not test_df.empty:
            test_df['bin'] = pd.qcut(test_df[feature].rank(method='first'), 10, labels=False, duplicates='drop') if feature in test_df.columns else 0
            
            train_dist = train_df['bin'].value_counts(normalize=True).sort_index()
            test_dist = test_df['bin'].value_counts(normalize=True).sort_index()
            
            # Align indexes
            all_bins = sorted(list(set(train_dist.index) | set(test_dist.index)))
            train_dist = train_dist.reindex(all_bins, fill_value=0.001)
            test_dist = test_dist.reindex(all_bins, fill_value=0.001)
            
            psi_vals = (test_dist - train_dist) * np.log(test_dist / train_dist)
            total_psi = psi_vals.sum()
        else:
            total_psi = 0
        
        return total_iv, total_psi

    def run_profiling(self, df, feature_list=None):
        """
        Runs the multi-horizon loop and generates metrics.
        """
        # Intelligent Scoping Loop
        if feature_list is None:
            feature_list = self._get_intelligent_feature_list(df.columns)
            logger.info(f"Intelligent Scoping: Selected {len(feature_list)} features based on SME logic.")
        
        # Split into global train/test by date (simplified Chronon split)
        # Assuming latest 3 months as test/monitoring
        max_date = df['COHORT_MONTH'].max()
        test_cutoff = max_date - pd.DateOffset(months=3)
        
        full_train_df = df[df['COHORT_MONTH'] <= test_cutoff].copy()
        full_test_df = df[df['COHORT_MONTH'] > test_cutoff].copy()

        results = []
        for target in self.targets:
            # Extract horizon (e.g. 8M) even if target is TARGET_HARD_8M or TARGET_SILENT_8M
            import re
            match = re.search(r'(\d+M)', target)
            hz_suffix = match.group(1) if match else "8M"
            excl_flag = f"EXCL_FLAG_3_{hz_suffix}"
            
            logger.info(f"Analyzing horizon: {target} using exclusion: {excl_flag}")
            
            # 1. Filter by exclusion flag (Leakage prevention)
            train_filtered = full_train_df[full_train_df[excl_flag] == 0].copy()
            test_filtered = full_test_df[full_test_df[excl_flag] == 0].copy()
            
            logger.info(f"Filtered {target} - Train: {len(train_filtered)}, Test: {len(test_filtered)}")
            
            if train_filtered.empty or target not in train_filtered.columns:
                logger.warning(f"Skipping {target}: No valid data found after filtering.")
                continue

            # 2. Balanced Undersampling (SageMaker practice for imbalanced labels)
            pos_cases = train_filtered[train_filtered[target] == 1]
            neg_cases = train_filtered[train_filtered[target] == 0]
            
            # Sampling 50k as per Gabriel's suggestion, or all available
            neg_sample_size = min(50000, len(neg_cases))
            balanced_train = pd.concat([
                pos_cases,
                neg_cases.sample(n=neg_sample_size, random_state=42)
            ]).reset_index(drop=True)
            
            # 3. Compute Metrics for each feature
            for feat in feature_list:
                if feat not in balanced_train.columns: continue
                
                # Check for NaNs and handle
                if balanced_train[feat].isna().all(): continue
                
                try:
                    iv, psi = self.calculate_iv_psi(balanced_train, test_filtered, feat, target)
                    
                    # Mutual Information (Statistical)
                    # Encode if necessary
                    X_mi = balanced_train[[feat]].fillna(balanced_train[feat].median() if np.issubdtype(balanced_train[feat].dtype, np.number) else -1)
                    if not np.issubdtype(X_mi[feat].dtype, np.number):
                        X_mi[feat] = LabelEncoder().fit_transform(X_mi[feat].astype(str))
                    
                    mi = mutual_info_classif(X_mi, balanced_train[target], discrete_features=False)[0]
                    
                    # Max Lift (at top decile)
                    stats = balanced_train.groupby(pd.qcut(balanced_train[feat].rank(method='first'), 10, labels=False, duplicates='drop'))[target].mean()
                    global_rate = balanced_train[target].mean()
                    max_lift = stats.max() / global_rate if global_rate > 0 else 0
                    
                    results.append({
                        'Feature': feat,
                        'Horizon': target,
                        'Total_IV': iv,
                        'Total_PSI': psi,
                        'Max_Lift': max_lift,
                        'Mutual_Information': mi,
                        'Missing_Pct': train_filtered[feat].isna().mean()
                    })
                except Exception as e:
                    logger.error(f"Error profiling {feat} for {target}: {str(e)}")
                    continue
        
        self.metrics_store = pd.DataFrame(results)
        return self.metrics_store

    def get_iv_comparison_pivot(self):
        if not self.metrics_store.empty:
            pivot = self.metrics_store.pivot(index='Feature', columns='Horizon', values='Total_IV')
            return pivot.sort_values(by=pivot.columns[0], ascending=False)
        return pd.DataFrame()
