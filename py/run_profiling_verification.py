from data_loader import get_snowpark_session, load_training_data
from feature_profiling import MultiHorizonProfiler
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_multi_horizon_analysis():
    session = get_snowpark_session()
    
    # 1. Load Data
    logger.info("Loading training data from Snowflake...")
    snowpark_df = load_training_data(session)
    
    # Take a representative random sample (e.g. 1%) for profiling
    # LIMIT 200000 could return a single temporal block, which might all be 'excluded'
    df = snowpark_df.sample(0.01).to_pandas()
    logger.info(f"Retrieved {len(df)} rows for analysis (Random Sample).")
    
    # Ensure column names are uppercase (Snowflake standard)
    df.columns = [c.upper() for c in df.columns]
    
    # 2. Define Features to Profile
    # Excluding keys, dates, and targets
    exclude_cols = [
        'ORG_URI', 'COHORT_MONTH', 'HISTORICAL_ATTRITION_DECISION',
        'TARGET_CURRENT_MONTH', 'TARGET_3M', 'TARGET_6M', 'TARGET_8M', 'TARGET_12M',
        'EXCL_FLAG_3_3M', 'EXCL_FLAG_3_6M', 'EXCL_FLAG_3_8M', 'EXCL_FLAG_3_12M',
        'EXCL_FLAG_1', 'EXCL_FLAG_2', 'FLIP_DATE', 'ATTRITION_DATE'
    ]
    feature_list = [c for c in df.columns if c not in exclude_cols]
    
    # 3. run Profiler
    profiler = MultiHorizonProfiler(targets=["TARGET_3M", "TARGET_6M", "TARGET_8M", "TARGET_12M"])
    metrics = profiler.run_profiling(df, feature_list)
    
    # 4. Save results
    metrics.to_csv('artifacts/multi_horizon_metrics.csv', index=False)
    logger.info("Metrics saved to artifacts/multi_horizon_metrics.csv")
    
    # 5. Display IV Comparison
    pivot = profiler.get_iv_comparison_pivot()
    print("\n--- IV Comparison across Horizons (Top 10) ---")
    print(pivot.head(10))
    
    return pivot

if __name__ == "__main__":
    run_multi_horizon_analysis()
