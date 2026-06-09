import os
import pathlib

# Windows compatibility patch for Snowpark OSError [WinError 123]
_orig_is_file = pathlib.Path.is_file
def _patched_is_file(self):
    try:
        str_path = str(self)
        if "<" in str_path or ">" in str_path:
            return False
        return _orig_is_file(self)
    except Exception:
        return False
pathlib.Path.is_file = _patched_is_file

os.environ["SNOWPARK_SKIP_SOURCE_CODE_COLLECTION"] = "True"

import pickle
import logging
import pandas as pd
import snowflake.snowpark.functions as F

from data_loader import get_snowpark_session, load_training_data
from feature_engineering import AttritionFeatureEngineer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# TreeSHAP Reason Codes
# ---------------------------------------------------------------------------

# ── Journey Decision Tree (Crawl → Walk → Run) ──────────────────────────────
# Implements the Tier × Cluster matrix agreed with Danielle Dane / Brad Siedler.
# IS_RAPID_ESCALATION=1 overrides all tiers → immediate reactive treatment.
# Key: (risk_tier, primary_attrition_category)
JOURNEY_MATRIX = {
    # ── Tier 1 — Top 15% combined score ─────────────────────────────────────
    (1, "Fee Related"):              ("T1-FEE",   "HIGH_PRIORITY_CALL",  "Senior outreach + fee waiver or AutoPay migration offer"),
    (1, "Declines at Pump"):         ("T1-PUMP",  "HIGH_PRIORITY_CALL",  "Proactive fleet control support call — resolve card block before customer seeks alternative"),
    (1, "Customer Service"):         ("T1-CS",    "HIGH_PRIORITY_CALL",  "Account executive rescue call — prior CRM case or escalating CS friction"),
    (1, "Fuel Volume / Purchasing"): ("T1-USAGE", "EMAIL_PLUS_SURVEY",   "Value reinforcement campaign + satisfaction survey to diagnose root cause"),
    # ── Tier 2 — Next 20% ────────────────────────────────────────────────────
    (2, "Fee Related"):              ("T2-FEE",   "AUTOMATED_EMAIL",     "Automated fee waiver offer or AutoPay migration nudge"),
    (2, "Declines at Pump"):         ("T2-PUMP",  "AUTOMATED_EMAIL",     "Fleet controls training email — self-service fix for card declines"),
    (2, "Customer Service"):         ("T2-CS",    "PERSONALIZED_EMAIL",  "Personalized service recovery email from account team"),
    (2, "Fuel Volume / Purchasing"): ("T2-USAGE", "EMAIL_PLUS_SURVEY",   "Value positioning email + survey"),
    # ── Tier 3 — Bottom 65% ──────────────────────────────────────────────────
    (3, "Fee Related"):              ("T3-FEE",   "PASSIVE_CONTENT",     "Billing tips and AutoPay awareness content"),
    (3, "Declines at Pump"):         ("T3-PUMP",  "PASSIVE_CONTENT",     "Self-service FAQ for fleet card controls"),
    (3, "Customer Service"):         ("T3-CS",    "PASSIVE_CONTENT",     "Satisfaction check-in email"),
    (3, "Fuel Volume / Purchasing"): ("T3-USAGE", "PASSIVE_CONTENT",     "Generic WEX value reinforcement"),
}
JOURNEY_RAPID_ESCALATION = ("RAPID-ESCALATION", "IMMEDIATE_ACTION", "Score increased ≥5pp — VAS team alert + CRM priority flag")


def _assign_journey(row):
    """Map a single prediction row to a journey ID, channel and treatment."""
    if row.get("IS_RAPID_ESCALATION", 0) == 1:
        jid, channel, treatment = JOURNEY_RAPID_ESCALATION
    else:
        key = (int(row.get("RISK_TIER", 3)), row.get("PRIMARY_ATTRITION_CATEGORY", "Fuel Volume / Purchasing"))
        jid, channel, treatment = JOURNEY_MATRIX.get(key, ("T3-USAGE", "PASSIVE_CONTENT", "Generic WEX value reinforcement"))
    return pd.Series({"JOURNEY_ID": jid, "JOURNEY_CHANNEL": channel, "JOURNEY_TREATMENT": treatment})


# Content strategy clusters defined with the marketing team.
# Every feature must belong to exactly one cluster.
# When adding new features, assign them here before using them in the model.
REASON_CODE_CATEGORY = {
    # ── Declining Usage (Green) ─────────────────────────────────────────────
    "SPEND_L3M_VS_BLENDED_RATIO":    "Fuel Volume / Purchasing",
    "ENT_GALLONS_VELOCITY_3V12":     "Fuel Volume / Purchasing",
    "ENT_GALLONS_VELOCITY_YOY":      "Fuel Volume / Purchasing",
    "ACTIVE_MONTHS_RATE_L12M":       "Fuel Volume / Purchasing",
    "ENT_GALLONS_AVG_3M":            "Fuel Volume / Purchasing",
    "CURRENT_VOLUME_VS_PEAK_PCT":    "Fuel Volume / Purchasing",
    "HISTORICAL_MAX_DROP_PCT":       "Fuel Volume / Purchasing",
    "ACCOUNT_COUNT":                 "Account Profile",
    "IS_TRUCKING_INDUSTRY":          "Account Profile",
    "IS_SMALL_BIZ":                  "Account Profile",
    # ── Declines at Pump (Red) ──────────────────────────────────────────────
    "DECLINED_TXN_RATE_L6M":         "Declines at Pump",
    "DECLINED_TXN_RATE_MTH":         "Declines at Pump",
    # ── Fee Related (Blue) ──────────────────────────────────────────────────
    "FEE_TO_REVENUE_RATIO_MTH":      "Fee Related",
    "FEE_TO_REVENUE_RATIO_MTH_LAG1": "Fee Related",
    "ENT_FEES_LAG1":                 "Fee Related",
    "FEE_RATIO_TREND_3M":            "Fee Related",
    # ── Customer Service (Orange) ───────────────────────────────────────────
    "ENT_CASE_TOTAL_LAG2":               "Customer Service",
    "ENT_CASE_TREND_3M":                 "Customer Service",
    "ENT_CASE_PER_1K_GALLONS":           "Customer Service",
    "MONTHS_SINCE_CS_TERMINATION_CASE":  "Customer Service",
}

ATTRITION_REASON_LABELS = {
    "SPEND_L3M_VS_BLENDED_RATIO":    "Declining Spend vs. Historical Baseline",
    "DECLINED_TXN_RATE_L6M":         "High Declined Transaction Rate at Pump",
    "ACTIVE_MONTHS_RATE_L12M":       "Reduced Fleet Activity (Past 12 Months)",
    "FEE_TO_REVENUE_RATIO_MTH":      "High Fee-to-Revenue Burden (Current Month)",
    "FEE_TO_REVENUE_RATIO_MTH_LAG1": "Elevated Fee-to-Revenue Burden (Prior Month)",
    "ENT_GALLONS_VELOCITY_3V12":     "Declining Fuel Volume Trend",
    "ENT_GALLONS_VELOCITY_YOY":      "Fuel Volume Declining Year-over-Year",
    "IS_TRUCKING_INDUSTRY":          "Trucking Industry Risk Profile",
    "ACCOUNT_COUNT":                 "Account Portfolio Concentration",
    "ENT_CASE_TOTAL_LAG2":           "Elevated Customer Service Interactions",
    "ENT_FEES_LAG1":                 "Recent Late or Service Fees Applied",
    "IS_SMALL_BIZ":                  "Small Business Vulnerability",
    "HISTORICAL_MAX_DROP_PCT":       "History of Sharp Volume Drops",
    "ENT_GALLONS_AVG_3M":            "Below-Average Fuel Consumption (3-Month Avg)",
    "ENT_CASE_TREND_3M":             "Escalating Customer Service Activity",
    "ENT_CASE_PER_1K_GALLONS":       "High CS Contact Rate Relative to Fuel Volume",
    "FEE_RATIO_TREND_3M":            "Fee Burden Increasing Over Time",
    "CURRENT_VOLUME_VS_PEAK_PCT":    "Volume Well Below Historical Peak",
    "DECLINED_TXN_RATE_MTH":         "Real-time Fuel Card Declined Spike",
    "MONTHS_SINCE_CS_TERMINATION_CASE": "Recent Customer Service Attrition Case",
}


def _compute_shap_reason_codes(model, X_features, prefix, top_n=3):
    """
    Compute TreeSHAP values for a LightGBM model and return the top-N attrition
    drivers per entity as human-readable reason-code columns.

    Positive SHAP values push the prediction toward attrition (class 1).
    Features are ranked by descending SHAP value so the most influential driver
    of risk appears first.

    Returns a DataFrame aligned to X_features.index with columns:
      {prefix}_REASON_1 .. {prefix}_REASON_{top_n}
      {prefix}_REASON_1_SHAP .. {prefix}_REASON_{top_n}_SHAP
    """
    try:
        import shap as _shap
    except ImportError:
        logger.warning("shap package not installed – reason codes unavailable. Run: pip install shap")
        n = len(X_features)
        cols = {}
        for i in range(1, top_n + 1):
            cols[f"{prefix}_REASON_{i}"] = [None] * n
            cols[f"{prefix}_REASON_{i}_SHAP"] = [None] * n
        return pd.DataFrame(cols, index=X_features.index)

    logger.info(f"Running TreeSHAP for {prefix} model on {len(X_features)} entities...")
    explainer = _shap.TreeExplainer(model)
    shap_values = explainer.shap_values(X_features)

    # LightGBM binary classifier: may return list [cls0_array, cls1_array] or a single 2-D array
    sv = shap_values[1] if isinstance(shap_values, list) else shap_values

    feature_names = X_features.columns.tolist()
    shap_df = pd.DataFrame(sv, columns=feature_names, index=X_features.index)

    out = {f"{prefix}_REASON_{i}": [] for i in range(1, top_n + 1)}
    out.update({f"{prefix}_REASON_{i}_SHAP": [] for i in range(1, top_n + 1)})
    out.update({f"{prefix}_REASON_{i}_CATEGORY": [] for i in range(1, top_n + 1)})

    for idx in shap_df.index:
        top_feats = shap_df.loc[idx].sort_values(ascending=False).head(top_n)
        for i, (feat, val) in enumerate(top_feats.items(), 1):
            label = ATTRITION_REASON_LABELS.get(feat, feat.replace("_", " ").title())
            category = REASON_CODE_CATEGORY.get(feat, "Fuel Volume / Purchasing")
            out[f"{prefix}_REASON_{i}"].append(label)
            out[f"{prefix}_REASON_{i}_SHAP"].append(round(float(val), 5))
            out[f"{prefix}_REASON_{i}_CATEGORY"].append(category)

    return pd.DataFrame(out, index=shap_df.index)


def predict_latest_snapshot(horizon="8M", min_gallons=100.0):
    """
    End-to-end inference script that pulls the most recent snapshot logic 
    and outputs probabilities using a trained LightGBM model.
    """
    horizon = horizon.upper()
    logger.info(f"Starting inference pipeline for {horizon} horizon on latest snapshot...")
    
    # 1. Load Model & Feature Engineer from local artifacts (simulating SageMaker Model Load)
    model_dir = os.environ.get('SM_MODEL_DIR', 'attrition_pipeline/artifacts')
    
    # Load HARD model
    try:
        with open(os.path.join(model_dir, "model_HARD.pkl"), "rb") as f:
            model_hard = pickle.load(f)
        with open(os.path.join(model_dir, "feature_engineer_HARD.pkl"), "rb") as f:
            fe_hard = pickle.load(f)
            
        # Load SILENT model
        with open(os.path.join(model_dir, "model_SILENT.pkl"), "rb") as f:
            model_silent = pickle.load(f)
        with open(os.path.join(model_dir, "feature_engineer_SILENT.pkl"), "rb") as f:
            fe_silent = pickle.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Missing model artifacts in {model_dir}. Please run train_model.py first.")
        
    # 2. Extract Latest Data from Snowflake
    session = get_snowpark_session()
    
    # 2a. Execute C6 Distance/Density Pipeline to ensure latest data
    logger.info("Executing C6 Distance/Density SQL Pipeline...")
    try:
        _script_dir = os.path.dirname(os.path.abspath(__file__))
        sql_path = os.path.normpath(os.path.join(_script_dir, '..', 'sql', 'c6_distance_density.sql'))
        from run_sql_updates import run_sql_file
        run_sql_file(session, sql_path)
    except Exception as e:
        logger.error(f"Failed to execute C6 Distance/Density pipeline: {e}")
        
    # 2b. Fetch Distance/Density Data into Pandas
    logger.info("Fetching Distance/Density metrics into Pandas...")
    try:
        # Load distance metrics, standardizing column names to uppercase for dictionary mapping
        distance_df = session.sql("SELECT ACCOUNTNUMBER, CLOSEST_SITE_DISTANCE_MILES, SITES_WITHIN_5_MI, SITES_WITHIN_25_MI, ACCOUNT_FOOTPRINT_CLASS FROM WORKSPACE.digitalda_stage.ACCOUNT_DISTANCE_DENSITY_C6").to_pandas()
        distance_df.columns = [c.upper() for c in distance_df.columns]
        distance_map = distance_df.set_index('ACCOUNTNUMBER').to_dict('index')
        logger.info(f"Loaded distance/density metrics for {len(distance_map)} accounts.")
    except Exception as e:
        logger.error(f"Failed to load distance/density metrics: {e}")
        distance_map = {}

    snowpark_df = load_training_data(session, table_name="WORKSPACE.digitalda_stage.ML_Model_Features_C5")
    
    # Keep active and valid accounts for scoring (we don't apply target exclusion flags since it's the future)
    # We strictly filter for entities that are currently Healthy and have at least 1 active account.
    # We apply this filter *before* finding the max date, to ensure we pick a month that actually has data.
    snowpark_df_valid = snowpark_df.filter(
        (F.col('EXCL_FLAG_1') == 0) & 
        (F.col('EXCL_FLAG_2') == 0) &
        (F.col('STATUS_ACTIVE_COUNT') > 0)
    )
    
    # Discover the latest cohort available that actually has valid data
    max_cohort_row = snowpark_df_valid.select(F.max("COHORT_MONTH")).collect()
    latest_cohort = max_cohort_row[0][0]
    
    if latest_cohort is None:
        raise ValueError("No valid cohorts found in the entire dataset with active/healthy entities.")
        
    logger.info(f"Identified latest valid COHORT_MONTH for inference: {latest_cohort}")
    
    # Standardize latest_cohort to a date string YYYY-MM-DD to avoid timestamp/timezone mismatch issues
    latest_cohort_str = latest_cohort.strftime("%Y-%m-%d") if hasattr(latest_cohort, 'strftime') else str(latest_cohort)[:10]
    logger.info(f"Using cohort date string for filters: {latest_cohort_str}")

    # Filter down to the specific latest valid snapshot
    snowpark_df_latest = snowpark_df_valid.filter(F.col("COHORT_MONTH") == latest_cohort)
    
    # 2c. Filter out entities that do not have volume_month in the latest cohort
    logger.info("Filtering entities to only those with a volume record in the current month...")
    try:
        active_orgs = (
            session.table("FINANCE_ANALYTICS.NAM_PORTFOLIO_METRICS.NAM_ACCOUNT_ATTRITION")
            .filter(F.date_trunc("month", F.col("VOLUME_MONTH")) == F.date_trunc("month", F.to_date(F.lit(latest_cohort_str))))
            .join(
                session.table("PREP.MDM_RELTIO.entity_wxaccountnumber"),
                F.col("SOURCE_ACCOUNT_ID") == F.col("ACCOUNTNUMBER"),
            )
            .join(
                session.table("PREP.MDM_RELTIO.f_entity_wxaccountnumber_organization_snapshot"),
                (F.col("URI") == F.col("ACCOUNT_URI")) &
                (F.to_date(F.lit(latest_cohort_str)) >= F.to_date(F.col("ROW_EFF_BEGIN_DTTM"))) &
                ((F.col("ROW_EFF_END_DTTM").is_null()) | (F.to_date(F.lit(latest_cohort_str)) < F.to_date(F.col("ROW_EFF_END_DTTM"))))
            )
            .select(F.col("ORGANIZATION_URI").alias("ORG_URI"))
            .distinct()
        )
            
        snowpark_df_latest = snowpark_df_latest.join(active_orgs, on="ORG_URI", how="inner")
        logger.info("Successfully filtered to active volume entities in Snowpark.")
    except Exception as e:
        logger.error(f"Failed to apply volume_month filter: {e}")
        raise
    
    row_count = snowpark_df_latest.count()
    logger.info(f"Downloading {row_count} rows for the snapshot {latest_cohort} into Pandas...")
    
    if row_count == 0:
        logger.error(f"ðŸ›‘ ABORTING: No valid entities ('Healthy', active accounts) found for the snapshot of {latest_cohort}.")
        logger.error("This usually happens if the data for this month has not yet been fully processed in Snowflake or if the filtering is too strict.")
        raise ValueError(f"The dataset is empty for date {latest_cohort} after applying filters. Aborting inference.")
    
    df = snowpark_df_latest.to_pandas()
    
    # Fetch current-day Risk Snapshot (Python-side) so we do not corrupt historical ML tables
    logger.info("Fetching current snapshot risk eligibility from DATAIKU_RAW.RISK_FRAUD.NAM_RDP...")
    try:
        risk_df = session.sql("SELECT CUST_ID, MAX(CREDIT_RISK_ELIGIBILITY_TAG) AS TAG FROM DATAIKU_RAW.RISK_FRAUD.NAM_RDP GROUP BY 1").to_pandas()
        risk_map = dict(zip(risk_df['CUST_ID'].astype(str), risk_df['TAG']))
    except Exception as e:
        logger.warning(f"Could not load risk mappings: {e}")
        risk_map = {}
        
    # session.close() # Moved to the end of the pipeline to allow writing results back to Snowflake

    # Load Program Tier mapping from local TSV (PROGRAM_ID â†’ Tier)
    # Use __file__ so the path resolves correctly regardless of the working directory
    tier_map = {}
    _script_dir = os.path.dirname(os.path.abspath(__file__))
    tiers_tsv_path = os.path.normpath(os.path.join(_script_dir, '..', 'data', 'program_tiers.tsv'))
    try:
        tiers_df = pd.read_csv(tiers_tsv_path, sep='\t', usecols=['PROGRAM_ID', 'Tier'], dtype=str)
        tiers_df.dropna(subset=['PROGRAM_ID', 'Tier'], inplace=True)
        tier_map = dict(zip(tiers_df['PROGRAM_ID'].str.strip(), tiers_df['Tier'].str.strip()))
        logger.info(f"Loaded {len(tier_map)} program-tier mappings from {tiers_tsv_path}")
    except Exception as e:
        logger.warning(f"Could not load program tier mappings from {tiers_tsv_path}: {e}")
    
    # 3. Apply Feature Engineering in Inference Mode
    logger.info("Applying feature transformations...")
    X_pred_hard = fe_hard.preprocess_data(df, is_training=False)
    X_pred_silent = fe_silent.preprocess_data(df, is_training=False)
    
    # Sync df to only include the rows that survived feature engineering filters
    df = df.loc[X_pred_hard.index].copy()
    
    # 4. Score Model Probabilities
    logger.info("Scoring probability predictions for HARD and SILENT models...")
    y_proba_hard = model_hard.predict_proba(X_pred_hard)[:, 1]
    y_proba_silent = model_silent.predict_proba(X_pred_silent)[:, 1]

    # 4b. Compute TreeSHAP reason codes for both models
    logger.info("Computing TreeSHAP reason codes for HARD and SILENT models...")
    shap_hard_df = _compute_shap_reason_codes(model_hard, X_pred_hard, prefix="HARD", top_n=3)
    shap_silent_df = _compute_shap_reason_codes(model_silent, X_pred_silent, prefix="SILENT", top_n=3)
    # Reindex to df's canonical index to handle any row-set differences between feature engineers
    shap_hard_df = shap_hard_df.reindex(df.index)
    shap_silent_df = shap_silent_df.reindex(df.index)

    # 5. Assemble final predictions output
    # Include total accounts and active/healthy accounts in the final csv
    cols_to_keep = ['ORG_URI', 'COHORT_MONTH']
    if 'ACCOUNT_COUNT' in df.columns:
        cols_to_keep.append('ACCOUNT_COUNT')
    if 'STATUS_ACTIVE_COUNT' in df.columns:
        cols_to_keep.append('STATUS_ACTIVE_COUNT')
        
    # Enrich account objects (from C3's ARRAY_AGG(OBJECT_CONSTRUCT)) with today's risk eligibility
    if 'ACCOUNT_ID_LIST' in df.columns:
        import json
        def _parse_acc_list(acc_val):
            """Parse the ACCOUNT_ID_LIST column into a list of dicts with account_id and program_id."""
            try:
                parsed = json.loads(acc_val) if isinstance(acc_val, str) else list(acc_val)
            except Exception:
                parsed = []
            # Normalise: each element is either a dict (new format from C3) or a plain id (legacy)
            result = []
            for item in parsed:
                if isinstance(item, dict):
                    raw_pid = item.get('program_id')
                    # Strip surrounding quotes and whitespace Snowflake sometimes injects
                    # e.g. '"1-1CA1YIW"' â†’ '1-1CA1YIW'
                    clean_pid = str(raw_pid).strip().strip('"').strip("'") if raw_pid is not None else None
                    result.append({
                        'account_id': str(item.get('account_id', '')),
                        'program_id': clean_pid,
                        'spend_mth': item.get('spend_mth', 0),
                        'gallons_mth': item.get('gallons_mth', 0),
                    })
                else:
                    result.append({'account_id': str(item), 'program_id': None})
            return result

        def _tier_priority(tier_str):
            """
            Returns a numeric priority for a tier string (lower = higher priority).
            Order: Tier 1 Universal < Tier 1 < Tier 2 Universal < Tier 2 < Tier 3 Universal < Tier 3 < ...
            Unrecognised / Unknown tiers land at the very bottom.
            """
            if not tier_str or tier_str.strip().lower() == 'unknown':
                return 9999
            t = tier_str.strip()
            is_universal = t.lower().endswith('universal')
            # Extract the numeric part, e.g. "Tier 2 Universal" -> 2
            import re as _re
            m = _re.search(r'\d+', t)
            tier_num = int(m.group()) if m else 9998
            # Universal variant gets priority within its tier level
            return tier_num * 2 - (1 if is_universal else 0)

        def enrich_accounts(acc_val):
            acc_list = _parse_acc_list(acc_val)
            details = [
                {
                    "account_id": acc['account_id'],
                    "credit_risk_eligibility": risk_map.get(acc['account_id'], "Unknown"),
                    "program_id": acc['program_id'],
                    "tier": tier_map.get(acc['program_id'], "Unknown") if acc['program_id'] else "Unknown",
                    "spend_last_month": acc.get('spend_mth', 0),
                    "gallons_last_month": acc.get('gallons_mth', 0),
                    "closest_site_distance_miles": distance_map.get(acc['account_id'], {}).get('CLOSEST_SITE_DISTANCE_MILES', None),
                    "sites_within_5_mi": distance_map.get(acc['account_id'], {}).get('SITES_WITHIN_5_MI', 0),
                    "sites_within_25_mi": distance_map.get(acc['account_id'], {}).get('SITES_WITHIN_25_MI', 0),
                    "account_footprint_class": distance_map.get(acc['account_id'], {}).get('ACCOUNT_FOOTPRINT_CLASS', 'Unknown'),
                }
                for acc in acc_list
            ]
            return json.dumps(details)
            
        def check_ineligible(acc_val):
            for acc in _parse_acc_list(acc_val):
                status = risk_map.get(acc['account_id'], "Unknown")
                # If ANY account is not explicitly 'Risk Eligible', flag the org
                if status.strip().lower() != "risk eligible":
                    return True
            return False

        def apply_selection_rules(details_json):
            """
            Rule engine combining Profitability Tiering and Geo Distance/Density.
            Returns a dictionary with the selected account_id and the rule_code.
            """
            try:
                accounts = json.loads(details_json) if isinstance(details_json, str) else details_json
            except Exception:
                return {"account_id": None, "rule_code": "ERROR"}
            if not accounts:
                return {"account_id": None, "rule_code": "ERROR"}

            # Sort by profitability tier (0 is highest priority)
            accounts_sorted = sorted(accounts, key=lambda a: _tier_priority(a.get('tier', 'Unknown')))
            contact_account = accounts_sorted[0]

            # R1: Single account entity
            if len(accounts) == 1:
                return {"account_id": contact_account.get('account_id'), "rule_code": "R1"}

            # Assumptions
            T_5MI_MIN = 3
            T_CLOSEST_MI = 2.0
            T_GEO_GAP = 3

            def get_sites_5mi(acc):
                val = acc.get('sites_within_5_mi')
                return int(val) if val is not None else None
                
            def get_closest_mi(acc):
                val = acc.get('closest_site_distance_miles')
                return float(val) if val is not None else None

            contact_sites = get_sites_5mi(contact_account)
            contact_closest = get_closest_mi(contact_account)

            # R5: No geo data
            if contact_sites is None or contact_closest is None:
                return {"account_id": contact_account.get('account_id'), "rule_code": "R5"}

            # R2: WELL-COVERED
            if contact_sites >= T_5MI_MIN and contact_closest <= T_CLOSEST_MI:
                return {"account_id": contact_account.get('account_id'), "rule_code": "R2"}

            # R3 & R4: POORLY COVERED. Look for Best Alternative Account.
            alt_accounts = [a for a in accounts_sorted[1:] if get_sites_5mi(a) is not None]
            
            if not alt_accounts:
                return {"account_id": contact_account.get('account_id'), "rule_code": "R4"}

            # Tie-breaking logic: Maximize sites_within_5_mi, then best Tier, then shortest closest_distance
            def alt_key(a):
                sites = get_sites_5mi(a)
                tier_p = _tier_priority(a.get('tier', 'Unknown'))
                dist = get_closest_mi(a)
                dist_val = dist if dist is not None else 9999.0
                return (sites, -tier_p, -dist_val)

            best_alt = max(alt_accounts, key=alt_key)
            geo_gap = get_sites_5mi(best_alt) - contact_sites

            # R3: SWITCH
            if geo_gap >= T_GEO_GAP:
                return {"account_id": best_alt.get('account_id'), "rule_code": "R3"}
            
            # R4: KEEP
            return {"account_id": contact_account.get('account_id'), "rule_code": "R4"}

        df['ACCOUNT_DETAILS_LIST'] = df['ACCOUNT_ID_LIST'].apply(enrich_accounts)
        df['HAS_INELIGIBLE_ACCOUNT'] = df['ACCOUNT_ID_LIST'].apply(check_ineligible)
        df['SELECTION_RESULT'] = df['ACCOUNT_DETAILS_LIST'].apply(apply_selection_rules)
        df['CONTACT_ACCOUNT_ID'] = df['SELECTION_RESULT'].apply(lambda x: x['account_id'])
        df['CONTACT_SELECTION_RULE'] = df['SELECTION_RESULT'].apply(lambda x: x['rule_code'])

        # Calculate entity-level totals from account details list
        import json as _json
        def get_total_gallons(details_json):
            try:
                accounts = _json.loads(details_json) if isinstance(details_json, str) else details_json
                if not accounts:
                    return 0.0
                return sum(float(acc.get('gallons_last_month', 0)) for acc in accounts)
            except Exception:
                return 0.0

        def get_total_spend(details_json):
            try:
                accounts = _json.loads(details_json) if isinstance(details_json, str) else details_json
                if not accounts:
                    return 0.0
                return sum(float(acc.get('spend_last_month', 0)) for acc in accounts)
            except Exception:
                return 0.0

        df['TOTAL_GALLONS_LAST_MONTH'] = df['ACCOUNT_DETAILS_LIST'].apply(get_total_gallons)
        df['TOTAL_SPEND_LAST_MONTH'] = df['ACCOUNT_DETAILS_LIST'].apply(get_total_spend)

        cols_to_keep.extend([
            'ACCOUNT_DETAILS_LIST',
            'HAS_INELIGIBLE_ACCOUNT',
            'CONTACT_ACCOUNT_ID',
            'CONTACT_SELECTION_RULE',
            'TOTAL_GALLONS_LAST_MONTH',
            'TOTAL_SPEND_LAST_MONTH'
        ])
        
    output_df = df[cols_to_keep].copy()

    # Attach TreeSHAP reason code columns (aligned to df.index)
    output_df = pd.concat([output_df, shap_hard_df, shap_silent_df], axis=1)

    # Fallback to zero if columns were not created (e.g. if ACCOUNT_ID_LIST is missing)
    if 'TOTAL_GALLONS_LAST_MONTH' not in output_df.columns:
        output_df['TOTAL_GALLONS_LAST_MONTH'] = 0.0
    if 'TOTAL_SPEND_LAST_MONTH' not in output_df.columns:
        output_df['TOTAL_SPEND_LAST_MONTH'] = 0.0
    
    output_df[f'PREDICTION_SCORE_HARD_{horizon}'] = y_proba_hard
    output_df[f'PREDICTION_SCORE_SILENT_{horizon}'] = y_proba_silent
    
    # Calculate individual ranks (1 = highest risk)
    output_df['PREDICTION_RANK_HARD'] = output_df[f'PREDICTION_SCORE_HARD_{horizon}'].rank(method='min', ascending=False).astype(int)
    output_df['PREDICTION_RANK_SILENT'] = output_df[f'PREDICTION_SCORE_SILENT_{horizon}'].rank(method='min', ascending=False).astype(int)

    # 1. Combined Attrition Score: Sum of probabilities (capped at 1.0)
    # This represents the total probability of 'Any Attrition' assuming disjoint events (per SQL logic)
    output_df[f'PREDICTION_SCORE_COMBINED_{horizon}'] = (
        output_df[f'PREDICTION_SCORE_HARD_{horizon}'] + 
        output_df[f'PREDICTION_SCORE_SILENT_{horizon}']
    ).clip(upper=1.0)

    # 2. Definitive Combined Rank: Based on Risk Tiers and historical ROI (volume)
    # Rank probabilities in percentile space (0.0 to 1.0, where 1.0 is highest risk in the current cohort)
    risk_pct = output_df[f'PREDICTION_SCORE_COMBINED_{horizon}'].rank(pct=True)

    # Segment into Risk Tiers: Tier 1 (High) = Top 15%, Tier 2 (Medium) = Next 20%, Tier 3 (Low) = Bottom 65%
    # Note: Lower tier number = higher priority
    output_df['RISK_TIER'] = pd.cut(
        risk_pct,
        bins=[-0.01, 0.65, 0.85, 1.01],
        labels=[3, 2, 1]
    ).astype(int)

    # Dominant attrition type: compare percentile ranks within each model's own distribution,
    # not raw probabilities. HARD attrition is much rarer than SILENT, so calibrated HARD
    # probabilities are always numerically lower — comparing raw scores would almost always
    # declare SILENT dominant even for accounts with extreme HARD risk.
    hard_pct  = output_df[f'PREDICTION_SCORE_HARD_{horizon}'].rank(pct=True)
    silent_pct = output_df[f'PREDICTION_SCORE_SILENT_{horizon}'].rank(pct=True)
    output_df['HARD_SCORE_PERCENTILE']   = (hard_pct * 100).round(1)
    output_df['SILENT_SCORE_PERCENTILE'] = (silent_pct * 100).round(1)
    output_df['DOMINANT_ATTRITION_TYPE'] = (hard_pct >= silent_pct).map({True: 'HARD', False: 'SILENT'})
    output_df['PRIMARY_ATTRITION_REASON'] = output_df.apply(
        lambda r: r['HARD_REASON_1'] if r['DOMINANT_ATTRITION_TYPE'] == 'HARD' else r['SILENT_REASON_1'],
        axis=1,
    )
    output_df['SECONDARY_ATTRITION_REASON'] = output_df.apply(
        lambda r: r['HARD_REASON_2'] if r['DOMINANT_ATTRITION_TYPE'] == 'HARD' else r['SILENT_REASON_2'],
        axis=1,
    )
    output_df['TERTIARY_ATTRITION_REASON'] = output_df.apply(
        lambda r: r['HARD_REASON_3'] if r['DOMINANT_ATTRITION_TYPE'] == 'HARD' else r['SILENT_REASON_3'],
        axis=1,
    )
    # Content strategy cluster for the primary driver — maps directly to Green/Red/Blue/Orange
    output_df['PRIMARY_ATTRITION_CATEGORY'] = output_df.apply(
        lambda r: r['HARD_REASON_1_CATEGORY'] if r['DOMINANT_ATTRITION_TYPE'] == 'HARD' else r['SILENT_REASON_1_CATEGORY'],
        axis=1,
    )

    # Calculate expected value metrics for additional context/visibility
    output_df['EXPECTED_GALLONS_AT_RISK'] = (
        output_df[f'PREDICTION_SCORE_COMBINED_{horizon}'] * 
        output_df['TOTAL_GALLONS_LAST_MONTH']
    )
    output_df['EXPECTED_SPEND_AT_RISK'] = (
        output_df[f'PREDICTION_SCORE_COMBINED_{horizon}'] * 
        output_df['TOTAL_SPEND_LAST_MONTH']
    )

    # ── Score Delta: how much has risk changed since the previous run? ────────
    # Needed for the journey decision tree: "rapid escalation" accounts get
    # high-priority reactive treatment regardless of their absolute tier.
    SCORE_COL = f'PREDICTION_SCORE_COMBINED_{horizon}'
    ESCALATION_THRESHOLD = 0.05   # ≥5pp absolute increase → rapid escalation flag
    try:
        prev_table = "WORKSPACE.digitalda_stage.ATTRITION_STAKEHOLDER_PREDICTIONS"
        prev_df = session.sql(f"""
            SELECT ORG_URI, {SCORE_COL} AS PREV_SCORE
            FROM {prev_table}
            WHERE COHORT_MONTH = (
                SELECT MAX(COHORT_MONTH) FROM {prev_table}
                WHERE COHORT_MONTH < (SELECT MAX(COHORT_MONTH) FROM {prev_table})
            )
        """).to_pandas()
        prev_df.columns = [c.upper() for c in prev_df.columns]
        output_df = output_df.merge(prev_df, on='ORG_URI', how='left')
        output_df['SCORE_DELTA'] = (
            output_df[SCORE_COL] - output_df['PREV_SCORE']
        ).round(4)
        output_df['IS_RAPID_ESCALATION'] = (
            output_df['SCORE_DELTA'] >= ESCALATION_THRESHOLD
        ).astype(int)
        output_df.drop(columns=['PREV_SCORE'], inplace=True)
        escalations = output_df['IS_RAPID_ESCALATION'].sum()
        logger.info(f"Score delta computed. {escalations} rapid escalations (delta ≥ {ESCALATION_THRESHOLD})")
    except Exception as e:
        logger.warning(f"Could not compute score delta (first run or missing history): {e}")
        output_df['SCORE_DELTA'] = None
        output_df['IS_RAPID_ESCALATION'] = 0

    # ── Journey routing: Tier × Cluster → JOURNEY_ID / CHANNEL / TREATMENT ──
    logger.info("Assigning journey routing (Tier × Cluster decision tree)...")
    journey_cols = output_df.apply(_assign_journey, axis=1)
    output_df = pd.concat([output_df, journey_cols], axis=1)

    # Sort sequentially by RISK_TIER (ascending), TOTAL_GALLONS_LAST_MONTH (descending), and combined score (descending)
    output_df = output_df.sort_values(
        by=['RISK_TIER', 'TOTAL_GALLONS_LAST_MONTH', f'PREDICTION_SCORE_COMBINED_{horizon}'], 
        ascending=[True, False, False]
    )
    
    # Assign unique, sequential rank
    output_df['PREDICTION_RANK_COMBINED'] = range(1, len(output_df) + 1)
    
    # Save output to artifacts directory or SageMaker output directory
    output_data_dir = os.environ.get('SM_OUTPUT_DATA_DIR', 'attrition_pipeline/artifacts')
    os.makedirs(output_data_dir, exist_ok=True)

    # Full predictions file (all orgs)
    out_path = os.path.join(output_data_dir, f"predictions_latest_{horizon}.csv")
    output_df.to_csv(out_path, index=False)
    logger.info(f"Saved {len(output_df)} predictions (full) to: {out_path}")

    # Stakeholder file â€” exclude orgs that have at least one ineligible account
    if 'HAS_INELIGIBLE_ACCOUNT' in output_df.columns:
        stakeholder_df = output_df[output_df['HAS_INELIGIBLE_ACCOUNT'] == False].copy()
    else:
        stakeholder_df = output_df.copy()

    # Filter out entities that do not have total_gallons_last_month >= min_gallons
    if 'TOTAL_GALLONS_LAST_MONTH' in stakeholder_df.columns:
        before_gallons_filter = len(stakeholder_df)
        stakeholder_df = stakeholder_df[stakeholder_df['TOTAL_GALLONS_LAST_MONTH'] >= min_gallons].copy()
        gallons_excluded = before_gallons_filter - len(stakeholder_df)
        logger.info(f"Excluded {gallons_excluded} orgs due to total gallons less than threshold of {min_gallons}")
    
    excluded = len(output_df) - len(stakeholder_df)

    # Add CONTACT_PROGRAM_ID: the program_id of the elected contact account
    if 'ACCOUNT_DETAILS_LIST' in stakeholder_df.columns and 'CONTACT_ACCOUNT_ID' in stakeholder_df.columns:
        import json as _json
        def _get_contact_program_id(row):
            try:
                accounts = _json.loads(row['ACCOUNT_DETAILS_LIST']) if isinstance(row['ACCOUNT_DETAILS_LIST'], str) else []
                contact_id = str(row['CONTACT_ACCOUNT_ID'])
                for acc in accounts:
                    if str(acc.get('account_id', '')) == contact_id:
                        return acc.get('program_id')
            except Exception:
                pass
            return None
        stakeholder_df['CONTACT_PROGRAM_ID'] = stakeholder_df.apply(_get_contact_program_id, axis=1)

    # Drop columns not needed for stakeholders, but keep the combined ones
    cols_to_drop = [c for c in ['PREDICTION_RANK_HARD', 'PREDICTION_RANK_SILENT', 'STATUS_ACTIVE_COUNT'] if c in stakeholder_df.columns]
    if cols_to_drop:
        stakeholder_df = stakeholder_df.drop(columns=cols_to_drop)

    stakeholder_path = os.path.join(output_data_dir, f"attrition_stakeholder_predictions_{horizon}.csv")
    stakeholder_df.to_csv(stakeholder_path, index=False)
    logger.info(
        f"Saved {len(stakeholder_df)} predictions (stakeholders, {excluded} orgs excluded) to: {stakeholder_path}"
    )

    # 6. Export Stakeholders Table to Snowflake
    target_table = "WORKSPACE.digitalda_stage.ATTRITION_STAKEHOLDER_PREDICTIONS"
    logger.info(f"Exporting stakeholder predictions to Snowflake table: {target_table}...")
    try:
        # Set database and schema context for the session to allow temp stage creation
        session.use_database("WORKSPACE")
        session.use_schema("DIGITALDA_STAGE")
        
        session.create_dataframe(stakeholder_df).write.mode("overwrite").save_as_table(target_table)
        logger.info(f"Successfully exported stakeholder predictions to {target_table}")
    except Exception as e:
        logger.error(f"Failed to export stakeholder predictions to Snowflake: {e}")

    session.close()
    logger.info(f"Pipeline execution complete. âœ…")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--horizon", type=str, default="8M", help="Target horizon model to use for inference")
    parser.add_argument("--min-gallons", type=float, default=100.0, help="Minimum last month total gallons for stakeholder export")
    args = parser.parse_args()
    
    predict_latest_snapshot(horizon=args.horizon, min_gallons=args.min_gallons)