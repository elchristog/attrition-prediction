# Attrition Prediction Pipeline

This repository contains the end-to-end pipeline for predicting customer attrition (Hard and Silent churn) using Snowpark and LightGBM.

## Project Structure

- `py/`: Python scripts for data loading, feature engineering, training, and inference.
- `sql/`: SQL scripts for the feature engineering waterfall in Snowflake (C1 to C5).
- `SHB/`: Supplemental SQL scripts.

## Key Features

- **Multi-Horizon Support**: Models for 3M, 6M, 8M, and 12M horizons.
- **Segmented Evaluation**: Performance metrics broken down by Hard Attrition and Silent Attrition.
- **Automated Feature Engineering**: Rolling windows, trend metrics, and behavioral signals.
- **Enriched Inference**: Output CSVs include account tiers and prioritized contact information.

## Usage

1. Run the SQL pipeline (C1 to C5) in Snowflake.
2. Train the model: `python py/train_model.py --horizon 8M`
3. Run inference: `python py/predict_model.py --horizon 8M`
