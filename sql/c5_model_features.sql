-- ════════════════════════════════════════════════════════════════════════════
-- C5 — ML_Model_Features_C5
-- Grain : ORG_URI × cohort_month
-- Purpose: Derive compound / ratio / trend / momentum features on top of
--          the ML_Attrition_Master_Table output from C4.
--
-- This version is aligned to the current C4 schema, using entity-level
-- current-month metrics plus rolling windows computed in C5.
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE WORKSPACE.digitalda_stage.ML_Model_Features_C5 AS

WITH

base AS (
    SELECT *
    FROM WORKSPACE.digitalda_stage.ML_Attrition_Master_Table
),

base_active AS (
    SELECT
        *,
        CASE WHEN COALESCE(ent_txns, 0) + COALESCE(ent_declined_txns, 0) > 0 THEN 1 ELSE 0 END AS entity_active_mth
    FROM base
),

rolling AS (
    SELECT
        *,
        LAG(ent_gross_spend, 1) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_gross_spend_l1m,
        LAG(ent_balance, 1) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_balance_l1m,
        LAG(ent_credit_limit, 1) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_credit_limit_l1m,
        AVG(ent_balance) OVER (PARTITION BY org_uri ORDER BY cohort_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ent_balance_avg_3m,
        AVG(ent_credit_limit) OVER (PARTITION BY org_uri ORDER BY cohort_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ent_credit_limit_avg_3m,
        AVG(ent_credit_limit) OVER (PARTITION BY org_uri ORDER BY cohort_month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS ent_credit_limit_avg_6m,
        AVG(ent_gross_spend) OVER (PARTITION BY org_uri ORDER BY cohort_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ent_gross_spend_avg_3m,
        AVG(ent_gross_spend) OVER (PARTITION BY org_uri ORDER BY cohort_month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS ent_gross_spend_avg_6m,
        AVG(ent_gross_spend) OVER (PARTITION BY org_uri ORDER BY cohort_month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS ent_gross_spend_avg_12m,
        AVG(ent_txns) OVER (PARTITION BY org_uri ORDER BY cohort_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ent_txns_avg_3m,
        AVG(ent_txns) OVER (PARTITION BY org_uri ORDER BY cohort_month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS ent_txns_avg_12m,
        SUM(ent_txns) OVER (PARTITION BY org_uri ORDER BY cohort_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ent_txns_sum_3m,
        SUM(ent_txns) OVER (PARTITION BY org_uri ORDER BY cohort_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS ent_txns_sum_6m,
        SUM(ent_declined_txns) OVER (PARTITION BY org_uri ORDER BY cohort_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ent_declined_txns_sum_3m,
        SUM(ent_declined_txns) OVER (PARTITION BY org_uri ORDER BY cohort_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS ent_declined_txns_sum_6m,
        AVG(ent_sr_count) OVER (PARTITION BY org_uri ORDER BY cohort_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ent_sr_count_avg_3m,
        AVG(ent_sr_count) OVER (PARTITION BY org_uri ORDER BY cohort_month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS ent_sr_count_avg_6m,
        SUM(ent_case_fee_waiver_count) OVER (PARTITION BY org_uri ORDER BY cohort_month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS ent_case_fee_waiver_sum_6m,
        SUM(CASE WHEN entity_active_mth = 1 THEN 1 ELSE 0 END) OVER (PARTITION BY org_uri ORDER BY cohort_month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS active_months_l6m,
        SUM(CASE WHEN entity_active_mth = 1 THEN 1 ELSE 0 END) OVER (PARTITION BY org_uri ORDER BY cohort_month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS active_months_l12m,
        LAG(account_count, 6) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS account_count_6m_ago,
        AVG(account_count) OVER (PARTITION BY org_uri ORDER BY cohort_month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS avg_account_count_l6m,
        -- New features: CS case trend & normalization
        AVG(ent_case_total) OVER (PARTITION BY org_uri ORDER BY cohort_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ent_case_total_avg_3m,
        AVG(ent_case_total) OVER (PARTITION BY org_uri ORDER BY cohort_month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS ent_case_total_avg_6m,
        -- New feature: fee ratio direction (baseline = avg from 3–5 months ago)
        AVG(ent_fees / NULLIF(ent_revenue, 0)) OVER (PARTITION BY org_uri ORDER BY cohort_month ROWS BETWEEN 5 PRECEDING AND 3 PRECEDING) AS fee_ratio_avg_3_5m,
        -- New feature: historical peak gallons (for current-vs-peak ratio)
        MAX(ent_gallons) OVER (PARTITION BY org_uri ORDER BY cohort_month ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS ent_gallons_hist_peak
    FROM base_active
),

same_period AS (
    SELECT
        b.org_uri,
        b.cohort_month,
        NULL AS spend_same_3m_last_year,
        NULL AS spend_same_6m_last_year,
        NULL AS gallons_same_3m_last_year,
        NULL AS txn_same_3m_last_year,
        NULL AS revenue_same_3m_last_year
    FROM rolling b
),

entity_history AS (
    SELECT
        org_uri,
        cohort_month,
        ROW_NUMBER() OVER (PARTITION BY org_uri ORDER BY cohort_month) AS months_of_history
    FROM base
),

activity_streak AS (
    SELECT
        org_uri,
        cohort_month,
        entity_active_mth,
        SUM(entity_active_mth) OVER (PARTITION BY org_uri ORDER BY cohort_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_active_cnt
    FROM base_active
),

streak_computed AS (
    SELECT
        org_uri,
        cohort_month,
        entity_active_mth,
        DATEDIFF(
            'month',
            MAX(CASE WHEN entity_active_mth = 1 THEN cohort_month END)
                OVER (PARTITION BY org_uri ORDER BY cohort_month ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
            cohort_month
        ) AS months_since_last_active,
        ROW_NUMBER() OVER (PARTITION BY org_uri, cum_active_cnt ORDER BY cohort_month) - 1 AS streak_raw
    FROM activity_streak
),

streak_final AS (
    SELECT
        org_uri,
        cohort_month,
        CASE WHEN entity_active_mth = 1 THEN 0 ELSE streak_raw END AS consecutive_inactive_months,
        months_since_last_active
    FROM streak_computed
),

cs_term AS (
    SELECT
        org_uri,
        cohort_month,
        MAX(CASE WHEN ent_case_termination_non_vas_count > 0 THEN cohort_month END)
            OVER (PARTITION BY org_uri ORDER BY cohort_month ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS last_term_case_month,
        MAX(CASE WHEN ent_case_termination_non_vas_count > 0 THEN 1 ELSE 0 END)
            OVER (PARTITION BY org_uri ORDER BY cohort_month ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS ever_had_termination_case
    FROM base_active
),

cs_term_final AS (
    SELECT
        org_uri,
        cohort_month,
        ever_had_termination_case,
        DATEDIFF('month', last_term_case_month, cohort_month) AS months_since_cs_termination_case
    FROM cs_term
)

SELECT
    r.ORG_URI,
    r.cohort_month,

    r.HISTORICAL_ATTRITION_DECISION,
    r.target_current_month,
    r.FLIP_DATE,
    r.ATTRITION_DATE,
    r.status_account_count,
    r.status_active_count,

    r.target_3m,
    r.target_6m,
    r.target_8m,
    r.target_12m,
    r.target_hard_8m,
    r.target_silent_8m,
    r.ACCOUNT_ID_LIST,

    r.excl_flag_1,
    r.excl_flag_2,
    r.excl_flag_3_6m,
    r.excl_flag_3_8m,
    r.excl_flag_3_12m,

    CASE WHEN eh.months_of_history < 6 THEN 1 ELSE 0 END AS EXCL_FLAG_HIST_6M,
    CASE WHEN eh.months_of_history < 12 THEN 1 ELSE 0 END AS EXCL_FLAG_HIST_12M,
    CASE WHEN eh.months_of_history < 15 THEN 1 ELSE 0 END AS EXCL_FLAG_HIST_15M,

    (r.ENT_BALANCE / NULLIF(r.ENT_CREDIT_LIMIT, 0)) AS CREDIT_UTILIZATION_RATIO,
    ((r.ENT_BALANCE / NULLIF(r.ENT_CREDIT_LIMIT, 0)) - (r.ent_balance_avg_3m / NULLIF(r.ent_credit_limit_avg_3m, 0))) AS UTILIZATION_TREND_3M,
    (r.ENT_DECLINED_TXNS / NULLIF(r.ENT_TXNS + r.ENT_DECLINED_TXNS, 0)) AS DECLINED_TXN_RATE_MTH,
    (r.ENT_DECLINED_TXNS_SUM_6M / NULLIF(r.ENT_TXNS_SUM_6M + r.ENT_DECLINED_TXNS_SUM_6M, 0)) AS DECLINED_TXN_RATE_L6M,
    (r.ENT_REVENUE / NULLIF(r.ENT_GALLONS, 0)) AS REVENUE_PER_GALLON_MTH,
    (r.ENT_FEES / NULLIF(r.ENT_REVENUE, 0)) AS FEE_TO_REVENUE_RATIO_MTH,
    LAG(r.ENT_FEES / NULLIF(r.ENT_REVENUE, 0), 1) OVER (PARTITION BY r.ORG_URI ORDER BY r.cohort_month) AS FEE_TO_REVENUE_RATIO_MTH_LAG1,
    NULL AS LATE_FEE_TO_TOTAL_FEE_RATIO,
    NULL AS SR_RESOLUTION_RATE_MTH,
    NULL AS SR_RESOLUTION_RATE_L6M,
    NULL AS CASE_DISTRESS_SCORE_MTH,
    (r.ENT_REVENUE / NULLIF(r.status_account_count, 0)) AS REVENUE_PER_ACCOUNT_MTH,

    ((r.ENT_GROSS_SPEND - r.ent_gross_spend_l1m) / NULLIF(r.ent_gross_spend_l1m, 0)) AS SPEND_MOM_GROWTH,
    (r.ent_gross_spend_avg_3m / NULLIF(0.5 * COALESCE(sp.spend_same_3m_last_year, r.ent_gross_spend_avg_3m) + 0.5 * r.ent_gross_spend_avg_12m, 0)) AS SPEND_L3M_VS_BLENDED_RATIO,
    (r.ent_gross_spend_avg_6m / NULLIF(0.5 * COALESCE(sp.spend_same_6m_last_year, r.ent_gross_spend_avg_6m) + 0.5 * r.ent_gross_spend_avg_12m, 0)) AS SPEND_L6M_VS_BLENDED_RATIO,
    (r.ent_gallons_avg_3m / NULLIF(0.5 * COALESCE(sp.gallons_same_3m_last_year, r.ent_gallons_avg_3m) + 0.5 * r.ent_gallons_avg_12m, 0)) AS GALLONS_L3M_VS_BLENDED_RATIO,
    (r.ent_txns_avg_3m / NULLIF(0.5 * COALESCE(sp.txn_same_3m_last_year, r.ent_txns_avg_3m) + 0.5 * r.ent_txns_avg_12m, 0)) AS TXN_COUNT_L3M_VS_BLENDED_RATIO,
    (r.active_months_l6m / 6.0) AS ACTIVE_MONTHS_RATE_L6M,
    (r.active_months_l12m / 12.0) AS ACTIVE_MONTHS_RATE_L12M,
    (r.ent_sr_count_avg_3m / NULLIF(r.ent_sr_count_avg_6m, 0)) AS SR_TREND_L3M_VS_L6M,
    ((r.ent_declined_txns_sum_3m / NULLIF(r.ent_txns_sum_3m + r.ent_declined_txns_sum_3m, 0)) - (r.ent_declined_txns_sum_6m / NULLIF(r.ent_txns_sum_6m + r.ent_declined_txns_sum_6m, 0))) AS DECLINED_TXN_TREND_3M,
    (r.ENT_CREDIT_LIMIT - r.ent_credit_limit_avg_6m) AS CR_LIMIT_CHANGE_L6M,
    CASE WHEN r.ent_case_fee_waiver_sum_6m > 0 THEN 1 ELSE 0 END AS REACTIVATION_FEE_FLAG_L6M,

    sf.months_since_last_active AS MONTHS_SINCE_LAST_ACTIVE,
    sf.consecutive_inactive_months AS CONSECUTIVE_INACTIVE_MONTHS,
    NULL AS PRICE_PER_GALLON_MTH,
    NULL AS NONFUEL_SPEND_SHARE_MTH,
    NULL AS PRODUCT_DEPTH_COUNT,
    ctf.months_since_cs_termination_case AS MONTHS_SINCE_CS_TERMINATION_CASE,
    COALESCE(ctf.ever_had_termination_case, 0) AS EVER_HAD_TERMINATION_CASE,
    NULL AS DPD_MAX_L6M,
    NULL AS DPD_MONTHS_ABOVE_30_L12M,
    NULL AS ACCOUNT_CHURN_RATE_L6M,
    CASE WHEN r.account_count_6m_ago IS NOT NULL THEN (r.ACCOUNT_COUNT - r.account_count_6m_ago) / NULLIF(r.account_count_6m_ago, 0) ELSE NULL END AS ACCOUNT_GROWTH_RATE_L6M,
    NULL AS ACQUISITION_CHANNEL_RISK_SCORE,
    NULL AS TENURE_BUCKET,
    NULL AS SEASONALITY_SPEND_RATIO,
    NULL AS FEE_WAIVER_FREQUENCY_L12M,
    r.ent_gallons_velocity_3v12 AS ENT_GALLONS_VELOCITY_3V12,
    r.ent_gallons_velocity_yoy AS ENT_GALLONS_VELOCITY_YOY,
    r.is_trucking_industry AS IS_TRUCKING_INDUSTRY,
    r.ACCOUNT_COUNT AS ACCOUNT_COUNT,
    r.ENT_CASE_TOTAL_LAG2 AS ENT_CASE_TOTAL_LAG2,
    r.ENT_FEES_LAG1 AS ENT_FEES_LAG1,
    r.ENT_FEES_LAG4 AS ENT_FEES_LAG4,
    r.is_small_biz AS IS_SMALL_BIZ,
    r.historical_max_drop_pct AS HISTORICAL_MAX_DROP_PCT,
    r.ent_gallons_avg_3m AS ENT_GALLONS_AVG_3M,

    -- ── NEW FEATURES (Gap analysis: enterprise CS intensity, fee trend, volume vs. peak) ──
    -- CS case trend: recent (L3M avg) vs. older baseline (L6M avg). >1.0 = escalating.
    (r.ent_case_total_avg_3m / NULLIF(r.ent_case_total_avg_6m, 0))     AS ENT_CASE_TREND_3M,
    -- CS case intensity normalized by volume: cases per 1,000 gallons (3M avg baseline).
    (r.ent_case_total / NULLIF(r.ent_gallons_avg_3m, 0) * 1000)        AS ENT_CASE_PER_1K_GALLONS,
    -- Fee ratio direction: current month ratio minus the 3–5 month-ago baseline.
    -- Positive = fee burden growing; captures deterioration before it crosses a hard threshold.
    ((r.ENT_FEES / NULLIF(r.ENT_REVENUE, 0)) - r.fee_ratio_avg_3_5m)  AS FEE_RATIO_TREND_3M,
    -- Current volume as a fraction of the entity's all-time historical peak gallons.
    -- <1.0 means the entity has not recovered to peak; <0.75 signals meaningful decline.
    (r.ent_gallons_avg_3m / NULLIF(r.ent_gallons_hist_peak, 0))        AS CURRENT_VOLUME_VS_PEAK_PCT
FROM rolling r
LEFT JOIN entity_history eh
    ON eh.ORG_URI = r.ORG_URI AND eh.cohort_month = r.cohort_month
LEFT JOIN same_period sp
    ON sp.ORG_URI = r.ORG_URI AND sp.cohort_month = r.cohort_month
LEFT JOIN streak_final sf
    ON sf.ORG_URI = r.ORG_URI AND sf.cohort_month = r.cohort_month
LEFT JOIN cs_term_final ctf
    ON ctf.ORG_URI = r.ORG_URI AND ctf.cohort_month = r.cohort_month
ORDER BY r.ORG_URI, r.cohort_month;
