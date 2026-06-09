-- ════════════════════════════════════════════════════════════════════════════
-- C3 — Enterprise_Entity_Features_Rolling
-- Grain : organization_uri × cohort_month
-- Purpose: Aggregates Account-level metrics (C2) to the Reltio Entity grain,
--          then computes rolling window statistical features (L3, L6, L12).
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE WORKSPACE.digitalda_stage.Enterprise_Entity_Features_Rolling AS

WITH account_features AS (
    SELECT * FROM WORKSPACE.digitalda_stage.Account_ID_features_monthly
),

-- ══════════════════════════════════════════════════════════════
-- CTE 1: Account-to-Entity Mapping (SCD2 Bridge)
-- ══════════════════════════════════════════════════════════════
anchor_records AS (
    SELECT 
        account_uri,
        organization_uri AS org_uri
    FROM PREP.MDM_RELTIO.f_entity_wxaccountnumber_organization_snapshot
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY account_uri 
        ORDER BY
            -- Priority 1: Was active exactly on March 1, 2026
            CASE WHEN row_eff_begin_dttm <= '2026-03-01'::DATE AND (row_eff_end_dttm IS NULL OR row_eff_end_dttm > '2026-03-01'::DATE) THEN 1
            -- Priority 2: Started after March 1, 2026 (get the earliesr one)
            WHEN row_eff_begin_dttm > '2026-03-01'::DATE THEN 2
            -- Priority 3: Ended before March 1, 2026 (get the latest one)
            ELSE 3 END,
        CASE WHEN row_eff_begin_dttm > '2026-03-01'::DATE THEN row_eff_begin_dttm END ASC,
        row_eff_end_dttm DESC
    ) = 1
),
historical_bridge AS (
    -- Standard SCD2 Condition for reliable periods (On or after March 1, 2026)
    SELECT
        c.cohort_month,
        snap.account_uri,
        snap.organization_uri AS org_uri
    FROM (SELECT DISTINCT cohort_month FROM account_features) c
    JOIN PREP.MDM_RELTIO.f_entity_wxaccountnumber_organization_snapshot snap
        ON c.cohort_month >= '2026-03-01'::DATE
        AND c.cohort_month >= snap.row_eff_begin_dttm
        AND (c.cohort_month < snap.row_eff_end_dttm OR snap.row_eff_end_dttm IS NULL)

    UNION ALL

    -- Retroactive patch for accounts that started before March 1, 2026
    SELECT 
        c.cohort_month,
        a.account_uri,
        a.org_uri
    FROM (SELECT DISTINCT cohort_month FROM account_features) c
    CROSS JOIN anchor_records a
    WHERE c.cohort_month < '2026-03-01'::DATE
),

-- ══════════════════════════════════════════════════════════════
-- CTE 2: Granular Entity-Account-Month Join
-- ══════════════════════════════════════════════════════════════
entity_account_base AS (
    SELECT
        hb.org_uri,
        af.*
    FROM account_features af
    JOIN PREP.MDM_RELTIO.entity_wxaccountnumber wx ON af.account_id = wx.accountnumber
    JOIN historical_bridge hb ON wx.uri = hb.account_uri AND af.cohort_month = hb.cohort_month
),

-- ══════════════════════════════════════════════════════════════
-- CTE 3: Entity Monthly Aggregation (Current Month Snapshot)
-- ══════════════════════════════════════════════════════════════
entity_monthly_snapshot AS (
    SELECT
        org_uri,
        cohort_month,
        -- Identity / Static flags (take MAX or MIN)
        MAX(partner_ind)          AS partner_ind,
        MAX(is_vip_ind)           AS is_vip_ind,
        MAX(is_govt_ind)          AS is_govt_ind,
        MAX(risk_grade_bucket)    AS risk_grade_bucket,
        MAX(is_fraud_ind)         AS is_fraud_ind,
        MAX(is_small_biz)         AS is_small_biz,
        MAX(is_trucking_industry) AS is_trucking_industry,
        MAX(tenure_months)        AS ent_tenure_max,
        COUNT(DISTINCT account_id) AS account_count,
        ARRAY_AGG(OBJECT_CONSTRUCT(
            'account_id', account_id::VARCHAR,
            'program_id', program_id::VARCHAR,
            'spend_mth', gross_spend_mth,
            'gallons_mth', gallons_mth
        )) AS account_id_list,
        
        -- Summation metrics
        SUM(gross_spend_mth)      AS ent_gross_spend,
        SUM(gallons_mth)          AS ent_gallons,
        SUM(revenue_mth)          AS ent_revenue,
        SUM(total_fee_mth)        AS ent_fees,
        SUM(transaction_count_mth) AS ent_txns,
        SUM(declined_txn_mth)     AS ent_declined_txns,
        SUM(sr_count_mth)         AS ent_sr_count,
        SUM(case_total_count)     AS ent_case_total,
        SUM(case_customer_current_count)        AS ent_case_customer_current_count,
        SUM(case_null_count)                    AS ent_case_null_count,
        SUM(case_account_count)                 AS ent_case_account_count,
        SUM(case_payment_billing_count)         AS ent_case_payment_billing_count,
        SUM(case_online_ivr_ma_count)           AS ent_case_online_ivr_ma_count,
        SUM(case_card_maintenance_count)        AS ent_case_card_maintenance_count,
        SUM(case_fraud_count)                   AS ent_case_fraud_count,
        SUM(case_card_declined_count)           AS ent_case_card_declined_count,
        SUM(case_fee_waiver_count)              AS ent_case_fee_waiver_count,
        SUM(case_junk_email_count)              AS ent_case_junk_email_count,
        SUM(case_payment_billing_rec_count)     AS ent_case_payment_billing_rec_count,
        SUM(case_termination_non_vas_count)     AS ent_case_termination_non_vas_count,
        SUM(case_authorization_count)           AS ent_case_authorization_count,
        SUM(case_application_fraud_count)       AS ent_case_application_fraud_count,
        SUM(case_claims_count)                  AS ent_case_claims_count,
        SUM(case_fees_count)                    AS ent_case_fees_count,
        SUM(case_disputes_count)                AS ent_case_disputes_count,
        SUM(case_pump_issues_count)             AS ent_case_pump_issues_count,
        SUM(case_total_excl_term_count)         AS ent_case_total_excl_term_count,
        SUM(case_other_count)                   AS ent_case_other_count,
        SUM(cr_limit_mth)         AS ent_credit_limit,
        SUM(outstanding_balance_mth) AS ent_balance,
        SUM(TOTAL_EXPOSURE_MTH)   AS ent_exposure,
        
        -- Count of active accounts
        SUM(is_active_month)      AS active_account_count

    FROM entity_account_base
    GROUP BY 1, 2
),

-- ══════════════════════════════════════════════════════════════
-- CTE 4: Base Trends (Prep for rolling stability metrics)
-- ══════════════════════════════════════════════════════════════
entity_base_trends AS (
    SELECT
        *,
        LAG(ent_gallons, 1) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_gallons_lag1,
        CASE 
            WHEN LAG(ent_gallons, 1) OVER (PARTITION BY org_uri ORDER BY cohort_month) > 0 
            THEN ent_gallons / LAG(ent_gallons, 1) OVER (PARTITION BY org_uri ORDER BY cohort_month)
            ELSE 1.0 
        END AS ent_gallons_mom_ratio
    FROM entity_monthly_snapshot
),

-- ══════════════════════════════════════════════════════════════
-- CTE 5: Rolling Window Calculations (L3, L6, L12)
-- ══════════════════════════════════════════════════════════════
entity_rolling AS (
    SELECT
        *,
        -- 3-Month Rolling Gallons
        AVG(ent_gallons) OVER (PARTITION BY org_uri ORDER BY cohort_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ent_gallons_avg_3m,
        MAX(ent_gallons) OVER (PARTITION BY org_uri ORDER BY cohort_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ent_gallons_max_3m,
        SUM(ent_gallons) OVER (PARTITION BY org_uri ORDER BY cohort_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ent_gallons_sum_3m,

        -- 6-Month Rolling Gallons
        AVG(ent_gallons) OVER (PARTITION BY org_uri ORDER BY cohort_month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS ent_gallons_avg_6m,
        
        -- 12-Month Rolling Gallons
        AVG(ent_gallons) OVER (PARTITION BY org_uri ORDER BY cohort_month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS ent_gallons_avg_12m,

        -- Spend trend (L3 vs L12)
        CASE 
            WHEN AVG(ent_gallons) OVER (PARTITION BY org_uri ORDER BY cohort_month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) > 0 
            THEN (AVG(ent_gallons) OVER (PARTITION BY org_uri ORDER BY cohort_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) / 
                  AVG(ent_gallons) OVER (PARTITION BY org_uri ORDER BY cohort_month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW))
            ELSE 1.0 
        END AS ent_gallons_velocity_3v12,

        -- ====================================================================
        -- NEW: Seasonality & Stability Metrics
        -- ====================================================================
        
        -- 1. YoY Velocity: L3 vs Same L3 Last Year
        CASE 
            WHEN AVG(ent_gallons) OVER (PARTITION BY org_uri ORDER BY cohort_month ROWS BETWEEN 14 PRECEDING AND 12 PRECEDING) > 0 
            THEN (AVG(ent_gallons) OVER (PARTITION BY org_uri ORDER BY cohort_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)) 
                / (AVG(ent_gallons) OVER (PARTITION BY org_uri ORDER BY cohort_month ROWS BETWEEN 14 PRECEDING AND 12 PRECEDING))
            ELSE 1.0 
        END AS ent_gallons_velocity_yoy,

        -- 2. Seasonal Behavior Flag
        CASE 
            WHEN LAG(ent_gallons, 12) OVER (PARTITION BY org_uri ORDER BY cohort_month) = 0 
                 AND LAG(ent_gallons, 6) OVER (PARTITION BY org_uri ORDER BY cohort_month) > 0
            THEN 1 
            ELSE 0 
        END AS is_historically_seasonal_flag,

        -- 3. The "Resurrection" Feature (Tolerancia a inactividad)
        SUM(CASE WHEN ent_gallons = 0 THEN 1 ELSE 0 END) OVER (
            PARTITION BY org_uri 
            ORDER BY cohort_month 
            ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING
        ) AS historical_zero_gallon_months,

        -- 4. Historical Volatility (Max Drop Tolerance)
        MIN(ent_gallons_mom_ratio) OVER (
            PARTITION BY org_uri 
            ORDER BY cohort_month 
            ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING
        ) AS historical_max_drop_pct,

        -- Delinquency Trend
        MAX(ent_exposure) OVER (PARTITION BY org_uri ORDER BY cohort_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ent_exposure_max_3m,
        AVG(ent_exposure) OVER (PARTITION BY org_uri ORDER BY cohort_month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS ent_exposure_avg_6m,
        
        -- Lagged Fees (T-1, T-2, T-4)
        LAG(ent_fees, 1) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_fees_lag1,
        LAG(ent_fees, 2) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_fees_lag2,
        LAG(ent_fees, 4) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_fees_lag4,

        -- Lagged Case Features (T-1, T-2)
        LAG(ent_case_total, 1) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_case_total_lag1,
        LAG(ent_case_total, 2) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_case_total_lag2,
        LAG(ent_case_customer_current_count, 1) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_case_customer_current_count_lag1,
        LAG(ent_case_customer_current_count, 2) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_case_customer_current_count_lag2,
        LAG(ent_case_null_count, 1) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_case_null_count_lag1,
        LAG(ent_case_null_count, 2) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_case_null_count_lag2,
        LAG(ent_case_account_count, 1) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_case_account_count_lag1,
        LAG(ent_case_account_count, 2) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_case_account_count_lag2,
        LAG(ent_case_payment_billing_count, 1) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_case_payment_billing_count_lag1,
        LAG(ent_case_payment_billing_count, 2) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_case_payment_billing_count_lag2,
        LAG(ent_case_online_ivr_ma_count, 1) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_case_online_ivr_ma_count_lag1,
        LAG(ent_case_online_ivr_ma_count, 2) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_case_online_ivr_ma_count_lag2,
        LAG(ent_case_card_maintenance_count, 1) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_case_card_maintenance_count_lag1,
        LAG(ent_case_card_maintenance_count, 2) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_case_card_maintenance_count_lag2,
        LAG(ent_case_fraud_count, 1) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_case_fraud_count_lag1,
        LAG(ent_case_fraud_count, 2) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_case_fraud_count_lag2,
        LAG(ent_case_card_declined_count, 1) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_case_card_declined_count_lag1,
        LAG(ent_case_card_declined_count, 2) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_case_card_declined_count_lag2,
        LAG(ent_case_fee_waiver_count, 1) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_case_fee_waiver_count_lag1,
        LAG(ent_case_fee_waiver_count, 2) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_case_fee_waiver_count_lag2,
        LAG(ent_case_junk_email_count, 1) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_case_junk_email_count_lag1,
        LAG(ent_case_junk_email_count, 2) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_case_junk_email_count_lag2,
        LAG(ent_case_payment_billing_rec_count, 1) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_case_payment_billing_rec_count_lag1,
        LAG(ent_case_payment_billing_rec_count, 2) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_case_payment_billing_rec_count_lag2,
        LAG(ent_case_termination_non_vas_count, 1) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_case_termination_non_vas_count_lag1,
        LAG(ent_case_termination_non_vas_count, 2) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_case_termination_non_vas_count_lag2,
        LAG(ent_case_authorization_count, 1) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_case_authorization_count_lag1,
        LAG(ent_case_authorization_count, 2) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_case_authorization_count_lag2,
        LAG(ent_case_application_fraud_count, 1) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_case_application_fraud_count_lag1,
        LAG(ent_case_application_fraud_count, 2) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_case_application_fraud_count_lag2,
        LAG(ent_case_claims_count, 1) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_case_claims_count_lag1,
        LAG(ent_case_claims_count, 2) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_case_claims_count_lag2,
        LAG(ent_case_fees_count, 1) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_case_fees_count_lag1,
        LAG(ent_case_fees_count, 2) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_case_fees_count_lag2,
        LAG(ent_case_disputes_count, 1) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_case_disputes_count_lag1,
        LAG(ent_case_disputes_count, 2) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_case_disputes_count_lag2,
        LAG(ent_case_pump_issues_count, 1) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_case_pump_issues_count_lag1,
        LAG(ent_case_pump_issues_count, 2) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_case_pump_issues_count_lag2,
        LAG(ent_case_total_excl_term_count, 1) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_case_total_excl_term_count_lag1,
        LAG(ent_case_total_excl_term_count, 2) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_case_total_excl_term_count_lag2,
        LAG(ent_case_other_count, 1) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_case_other_count_lag1,
        LAG(ent_case_other_count, 2) OVER (PARTITION BY org_uri ORDER BY cohort_month) AS ent_case_other_count_lag2        
    FROM entity_base_trends
)

SELECT * FROM entity_rolling;
