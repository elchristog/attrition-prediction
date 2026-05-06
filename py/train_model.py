import os
import pathlib

# Windows compatibility patch for Snowpark OSError [WinError 123]
# Intercepts invalid path checks during AST collection for modules like <frozen importlib>
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
from lightgbm import LGBMClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, roc_auc_score, average_precision_score, precision_recall_curve
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import seaborn as sns

from data_loader import get_snowpark_session, load_training_data
import feature_engineering

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def lift_chart_plot(plot_name: str, decile_df: pd.DataFrame, x_axis: str):
    event_rate = decile_df["TOTAL_EVENT_RATE"].astype(float).mean()

    # Build event rate plot
    fig = plt.figure(figsize=(12,4))

    # Plot barplot containing number of observations
    plt.bar(decile_df.index, decile_df["COUNT"], color="lightgray")

    # Add ticks & laels to axis
    plt.xlabel("Model Sorted Predictions (Low → High)")
    plt.ylabel("# Observations")

    # Format x-axis labels to show 3 decimal places
    x_labels = []
    for label in decile_df[x_axis]:
        # Extract the minimum and maximum probabilities from the string
        min_prob, max_prob = map(float, label[1:-1].split(" - ")) 
        avg_prob = (min_prob + max_prob) / 2
        x_labels.append(f"{avg_prob:.3f}")
    plt.xticks(decile_df.index, x_labels, rotation=45, ha='right', rotation_mode='anchor')

    plt.title(f"Actual vs. Predicted Lift Chart - {plot_name}")

    # Mirror plot and add event rates
    plt2 = plt.twinx()
    plt2.set_ylabel("Event rate")
    plt2.set_ylim(ymin=0, ymax=decile_df["AVG_PROB"].max() + 0.05)
    plt2.set_yticks(np.arange(0, decile_df["AVG_PROB"].max() + 0.05, step=0.05))
    plt2.plot(
        decile_df.index, decile_df["DEFAULT_RATE"], label="event_rate", marker="o"
    )

    # add average prediction
    plt2.plot(
        decile_df.index,
        decile_df["AVG_PROB"],
        label="average_prediction",
        marker="x",
        linestyle=":",
        color="black"
    )

    # Add global event rate as baseline
    plt2.plot(
        [min(decile_df.index) - 1, max(decile_df.index) + 1],
        [event_rate, event_rate],
        color="darkgrey",
        lw=1,
        linestyle="--",
        label=f"total_event_rate\n({'{:.1%}'.format(event_rate)})",
    )
    plt2.legend(loc=0)
    plt2.yaxis.set_major_formatter(ticker.PercentFormatter(xmax=1, decimals=0))

    plt2.yaxis.grid(False)
    plt2.set_xlim([min(decile_df.index) - 0.5, max(decile_df.index) + 0.5])

    plt.tight_layout()  # Adjust layout to prevent labels from overlapping
    
    # Save instead of show for the pipeline
    output_data_dir = os.environ.get('SM_OUTPUT_DATA_DIR', 'attrition_pipeline/artifacts')
    os.makedirs(output_data_dir, exist_ok=True)
    plt.savefig(os.path.join(output_data_dir, f"lift_chart_{plot_name.replace(' ', '_')}.png"))
    plt.close(fig)

def train_attrition_model(horizon="8M"):
    """
    End-to-end model training pipeline for a specific churn horizon.
    """
    horizon = horizon.upper()
    logger.info(f"Starting training pipeline for {horizon} horizon...")
    
    # 1. Load Data
    session = get_snowpark_session()
    snowpark_df = load_training_data(session, table_name="WORKSPACE.digitalda_stage.ML_Model_Features_C5")
    # 1.1 Apply Business Exclusion Filters (SME Logic)
    # flag 1: noise, flag 2: post-attrition, flag 3: insufficient future/leakage
    excl_flag_horizon = f"EXCL_FLAG_3_{horizon}"
    
    import snowflake.snowpark.functions as F
    
    initial_len = snowpark_df.count()
    
    snowpark_df = snowpark_df.filter(
        (F.col('EXCL_FLAG_1') == 0) & 
        (F.col('EXCL_FLAG_2') == 0) & 
        (F.col(excl_flag_horizon) == 0)
    )
    
    filtered_len = snowpark_df.count()
    logger.info(f"Data Filtering: Removed {initial_len - filtered_len} rows. Clean set: {filtered_len} rows.")

    df = snowpark_df.to_pandas()
    session.close()

    # 2. Feature Engineering
    fe = feature_engineering.AttritionFeatureEngineer(horizon=horizon)
    X, y = fe.preprocess_data(df)
    
    # 2.1 Prepare segmented targets for evaluation (Hard vs Silent)
    target_hard_col = f"TARGET_HARD_{horizon}"
    target_silent_col = f"TARGET_SILENT_{horizon}"
    
    # Sync with filtered X index
    y_hard = df.loc[X.index, target_hard_col] if target_hard_col in df.columns else None
    y_silent = df.loc[X.index, target_silent_col] if target_silent_col in df.columns else None
    
    # 3. Group-level Train/Test Split (stratified by whether the org ever had attrition)
    # We use ORG_URI to ensure all history of an entity goes to either train or test
    org_uri_series = df.loc[X.index, 'ORG_URI']
    
    # Group by ORG_URI and find if they ever had attrition (max target)
    org_df = pd.DataFrame({'ORG_URI': org_uri_series.values, 'TARGET': y}, index=X.index)
    org_attrition = org_df.groupby('ORG_URI')['TARGET'].max()
    
    # Split the unique organizations with stratification
    train_orgs, test_orgs = train_test_split(
        org_attrition.index, 
        test_size=0.2, 
        random_state=42, 
        stratify=org_attrition.values
    )
    
    # Create train and test masks for the rows
    train_mask = org_df['ORG_URI'].isin(train_orgs)
    test_mask = org_df['ORG_URI'].isin(test_orgs)
    
    X_train, X_test = X[train_mask], X[test_mask]
    y_train, y_test = y[train_mask.values], y[test_mask.values]
    
    # Mask segmented targets
    y_test_hard = None
    y_test_silent = None
    
    if y_hard is not None:
        y_test_hard = y_hard[test_mask]
    if y_silent is not None:
        y_test_silent = y_silent[test_mask]
    
    # 4. Model Training
    logger.info("Training LightGBM model...")
    model = LGBMClassifier(
        n_estimators=100,
        max_depth=5,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42
    )
    
    model.fit(X_train, y_train)
    
    # 5. Evaluation
    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]
    
    auc_roc = roc_auc_score(y_test, y_proba)
    auc_pr = average_precision_score(y_test, y_proba)
    
    logger.info(f"Model Results: AUC-ROC: {auc_roc:.4f}, PR-AUC: {auc_pr:.4f}")
    
    # 5.1 Segmented Evaluation
    if y_test_hard is not None:
        auc_hard = roc_auc_score(y_test_hard, y_proba)
        pr_hard = average_precision_score(y_test_hard, y_proba)
        logger.info(f"Segmented Results [HARD ATTRITION]: AUC-ROC: {auc_hard:.4f}, PR-AUC: {pr_hard:.4f}")
        
    if y_test_silent is not None:
        auc_silent = roc_auc_score(y_test_silent, y_proba)
        pr_silent = average_precision_score(y_test_silent, y_proba)
        logger.info(f"Segmented Results [SILENT ATTRITION]: AUC-ROC: {auc_silent:.4f}, PR-AUC: {pr_silent:.4f}")

    logger.info("\n" + classification_report(y_test, y_pred))
    
    # 6. Decile Analysis / Lift Chart
    results = pd.DataFrame({'y_true': y_test, 'proba': y_proba})
    results['prob_rank'] = results['proba'].rank(method='first', ascending=True)
    results['decile'] = pd.qcut(results['prob_rank'], 10, labels=False)
    
    decile_df = results.groupby('decile').agg(
        COUNT=('y_true', 'size'),
        DEFAULT_RATE=('y_true', 'mean'),
        AVG_PROB=('proba', 'mean'),
        MIN_PROB=('proba', 'min'),
        MAX_PROB=('proba', 'max')
    ).reset_index(drop=True)
    
    decile_df['TOTAL_EVENT_RATE'] = results['y_true'].mean()
    decile_df['prob_range'] = decile_df.apply(lambda row: f"[{row['MIN_PROB']:.5f} - {row['MAX_PROB']:.5f}]", axis=1)
    
    logger.info("Generating Lift Chart...")
    lift_chart_plot(f"LightGBM Base Model Horizon {horizon}", decile_df, "prob_range")
    
    # 7. Save Artifacts
    # SageMaker paths
    model_dir = os.environ.get('SM_MODEL_DIR', 'attrition_pipeline/artifacts')
    output_data_dir = os.environ.get('SM_OUTPUT_DATA_DIR', 'attrition_pipeline/artifacts')
    
    os.makedirs(model_dir, exist_ok=True)
    os.makedirs(output_data_dir, exist_ok=True)
    
    with open(os.path.join(model_dir, "model.pkl"), "wb") as f:
        pickle.dump(model, f)
        
    with open(os.path.join(model_dir, "feature_engineer.pkl"), "wb") as f:
        pickle.dump(fe, f)
        
    logger.info(f"Saved model and feature engineer to {model_dir}")
    
    # 8. Feature Importance Plot
    importance_df = pd.DataFrame({
        'feature': X.columns,
        'importance': model.feature_importances_
    }).sort_values(by='importance', ascending=False)
    
    plt.figure(figsize=(10, 8))
    sns.barplot(x='importance', y='feature', data=importance_df)
    plt.title("Feature Importances - Baseline Model")
    plt.yticks(fontsize=9)  # Reduce font size
    plt.tight_layout()      # Automatically adjust subplots to give room for labels
    plt.savefig(os.path.join(output_data_dir, "feature_importance.png"), bbox_inches='tight')
    plt.close()
    logger.info(f"Saved feature importance plot to {output_data_dir}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--horizon", type=str, default="8M", help="Target horizon (3M, 6M, 8M, 12M)")
    args = parser.parse_args()
    
    train_attrition_model(horizon=args.horizon)
