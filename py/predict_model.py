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

def predict_latest_snapshot(horizon="8M"):
    """
    End-to-end inference script that pulls the most recent snapshot logic 
    and outputs probabilities using a trained LightGBM model.
    """
    horizon = horizon.upper()
    logger.info(f"Starting inference pipeline for {horizon} horizon on latest snapshot...")
    
    # 1. Load Model & Feature Engineer from local artifacts (simulating SageMaker Model Load)
    model_dir = os.environ.get('SM_MODEL_DIR', 'attrition_pipeline/artifacts')
    
    model_path = os.path.join(model_dir, "model.pkl")
    fe_path = os.path.join(model_dir, "feature_engineer.pkl")
    
    if not os.path.exists(model_path) or not os.path.exists(fe_path):
        raise FileNotFoundError(f"Missing model artifacts in {model_dir}. Please run train_model.py first.")
        
    with open(model_path, "rb") as f:
        model = pickle.load(f)
        
    with open(fe_path, "rb") as f:
        fe = pickle.load(f)
        
    # 2. Extract Latest Data from Snowflake
    session = get_snowpark_session()
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
    
    # Filter down to the specific latest valid snapshot
    snowpark_df_latest = snowpark_df_valid.filter(F.col("COHORT_MONTH") == latest_cohort)
    
    row_count = snowpark_df_latest.count()
    logger.info(f"Downloading {row_count} rows for the snapshot {latest_cohort} into Pandas...")
    
    if row_count == 0:
        logger.error(f"🛑 ABORTANDO: No se encontraron entidades válidas ('Healthy', cuentas activas) para el snapshot del {latest_cohort}.")
        logger.error("Esto suele suceder si los datos de este mes aún no se han procesado por completo en Snowflake o si el filtrado es muy estricto.")
        raise ValueError(f"El dataset está vacío para la fecha {latest_cohort} después de aplicar los filtros. Abortando inferencia.")
    
    df = snowpark_df_latest.to_pandas()
    
    # Fetch current-day Risk Snapshot (Python-side) so we do not corrupt historical ML tables
    logger.info("Fetching current snapshot risk eligibility from DATAIKU_RAW.RISK_FRAUD.NAM_RDP...")
    try:
        risk_df = session.sql("SELECT CUST_ID, MAX(CREDIT_RISK_ELIGIBILITY_TAG) AS TAG FROM DATAIKU_RAW.RISK_FRAUD.NAM_RDP GROUP BY 1").to_pandas()
        risk_map = dict(zip(risk_df['CUST_ID'].astype(str), risk_df['TAG']))
    except Exception as e:
        logger.warning(f"Could not load risk mappings: {e}")
        risk_map = {}
        
    session.close()

    # Load Program Tier mapping from local TSV (PROGRAM_ID → Tier)
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
    # Using the pre-fit label encoders and feature selectors from training
    logger.info("Applying feature transformations...")
    X_pred = fe.preprocess_data(df, is_training=False)
    
    # Sync df to only include the rows that survived feature engineering filters
    df = df.loc[X_pred.index].copy()
    
    # 4. Score Model Probabilities
    logger.info("Scoring probability predictions using LightGBM model...")
    y_proba = model.predict_proba(X_pred)[:, 1]
    
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
                    # e.g. '"1-1CA1YIW"' → '1-1CA1YIW'
                    clean_pid = str(raw_pid).strip().strip('"').strip("'") if raw_pid is not None else None
                    result.append({
                        'account_id': str(item.get('account_id', '')),
                        'program_id': clean_pid,
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

        def select_contact_account(details_json):
            """
            From the already-enriched ACCOUNT_DETAILS_LIST JSON, pick the account_id
            of the account we should contact.
            - Single account: that account.
            - Multiple accounts: the one with the highest-priority tier.
              Priority: Tier 1 Universal > Tier 1 > Tier 2 Universal > Tier 2 > ...
            """
            try:
                accounts = json.loads(details_json) if isinstance(details_json, str) else details_json
            except Exception:
                return None
            if not accounts:
                return None
            best = min(accounts, key=lambda a: _tier_priority(a.get('tier', 'Unknown')))
            return best.get('account_id')

        df['ACCOUNT_DETAILS_LIST'] = df['ACCOUNT_ID_LIST'].apply(enrich_accounts)
        df['HAS_INELIGIBLE_ACCOUNT'] = df['ACCOUNT_ID_LIST'].apply(check_ineligible)
        df['CONTACT_ACCOUNT_ID'] = df['ACCOUNT_DETAILS_LIST'].apply(select_contact_account)

        cols_to_keep.append('ACCOUNT_DETAILS_LIST')
        cols_to_keep.append('HAS_INELIGIBLE_ACCOUNT')
        cols_to_keep.append('CONTACT_ACCOUNT_ID')
        
    output_df = df[cols_to_keep].copy()
    output_df[f'PREDICTION_SCORE_{horizon}'] = y_proba
    
    # Sort by descending probability for quick prioritization
    output_df = output_df.sort_values(by=f'PREDICTION_SCORE_{horizon}', ascending=False)
    output_df['PREDICTION_RANK'] = output_df[f'PREDICTION_SCORE_{horizon}'].rank(method='min', ascending=False).astype(int)
    
    # Save output to artifacts directory or SageMaker output directory
    output_data_dir = os.environ.get('SM_OUTPUT_DATA_DIR', 'attrition_pipeline/artifacts')
    os.makedirs(output_data_dir, exist_ok=True)

    # Full predictions file (all orgs)
    out_path = os.path.join(output_data_dir, f"predictions_latest_{horizon}.csv")
    output_df.to_csv(out_path, index=False)
    logger.info(f"Saved {len(output_df)} predictions (full) to: {out_path}")

    # Stakeholder file — exclude orgs that have at least one ineligible account
    if 'HAS_INELIGIBLE_ACCOUNT' in output_df.columns:
        stakeholder_df = output_df[output_df['HAS_INELIGIBLE_ACCOUNT'] == False].copy()
        excluded = len(output_df) - len(stakeholder_df)
    else:
        stakeholder_df = output_df.copy()
        excluded = 0

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

    # Drop columns not needed for stakeholders
    cols_to_drop = [c for c in ['PREDICTION_RANK', 'STATUS_ACTIVE_COUNT'] if c in stakeholder_df.columns]
    if cols_to_drop:
        stakeholder_df = stakeholder_df.drop(columns=cols_to_drop)

    stakeholder_path = os.path.join(output_data_dir, f"predictions_latest_{horizon}_stakeholders.csv")
    stakeholder_df.to_csv(stakeholder_path, index=False)
    logger.info(
        f"Saved {len(stakeholder_df)} predictions (stakeholders, {excluded} orgs excluded) to: {stakeholder_path}"
    )

    logger.info(f"Pipeline execution complete. ✅")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--horizon", type=str, default="8M", help="Target horizon model to use for inference")
    args = parser.parse_args()
    
    predict_latest_snapshot(horizon=args.horizon)
