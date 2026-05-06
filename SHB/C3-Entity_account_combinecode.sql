-- ════════════════════════════════════════════════════════════════════════════
-- C3 — Entity_ID_features_monthly
-- Grain : ORG_URI × cohort_month
-- Purpose:
--   STEP 1  Merge account-month raw snapshot (C2) with entity-account map (C1)
--   STEP 2  Aggregate to entity × month (raw sums / counts / flags)
--           → includes small-biz account counts
--   STEP 3  NEW — compute rolling L1/L3/L6/L12 windows on the ENTITY-month
--           series 
-- Change log:
--   • Rolling windows moved here from C2 and computed on entity totals
--   • is_small_biz carried from C2; entity-level counts added in Step 2
--   • Old "SUM(account-level rolling)" columns replaced with proper entity windows
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE WORKSPACE.digitalda_stage.Entity_ID_features_monthly AS

WITH

-- ══════════════════════════════════════════════════════════════
-- STEP 1: Merge account-month table with entity-account mapping
-- ══════════════════════════════════════════════════════════════
merged AS (
    SELECT
        -- ── JOIN KEYS ──
        m.account_id,
        m.cohort_month,

        -- ── ENTITY MAPPING COLUMNS ──
        c.ORG_URI,
        c.OBS_DATE,
        c.ACCOUNT_CLOSED_DATE        AS entity_account_closed_date,
        c.ATTRITION_TYPE,
        c.OUTSTANDING_CARD_COUNT     AS entity_outstanding_card_count,
        c.ACTIVE_CARD_COUNT          AS entity_active_card_count,
        c.PURCHASE_GALLONS_QTY       AS entity_purchase_gallons_qty,
        c.WEX_TRANSACTION_COUNT      AS entity_wex_transaction_count,
        c.ACCOUNT_TENURE_MONTHS      AS entity_account_tenure_months,
        c.CR_LIMIT                   AS entity_cr_limit,
        c.HAS_CREDIT_RISK            AS entity_has_credit_risk,

        -- ── STATIC / IDENTITY ──
        m.business_entity,
        m.business_program_name,
        m.partner,
        m.partner_ind,
        m.is_vip_ind,
        m.is_govt_ind,
        m.acquisition_year,
        m.acquisition_month,
        m.account_open_date,
        m.account_closed_date,
        m.account_status,
        m.tenure_months,
        m.acquisition_fico,
        m.credit_bureau_score,
        m.approved_credit_limit,
        m.app_fleet_size,
        m.years_in_business,
        m.risk_grade,
        m.risk_grade_bucket,
        m.campaign_type_sf,
        m.fraud_decision,
        m.sales_type,
        m.campaign_tactic,
        m.is_fraud_ind,
        m.naics_description,
        m.naics_industry_code,
        m.is_trucking_industry,
        m.first_transaction_dt,
        m.days_to_first_txn,

        -- ── SMALL BUSINESS FLAG (from C2) ──
        m.is_small_biz,

        -- ── FLEET ──
        m.max_active_cardcnt_mth,
        m.outstanding_cardcnt_mth,

        -- ── CURRENT MONTH RAW METRICS ──
        m.declined_txn_mth,
        m.gross_spend_mth,
        m.fuel_spend_mth,
        m.INN_spend_mth,
        m.OON_spend_mth,
        m.gallons_mth,
        m.INN_gallons_mth,
        m.OON_gallons_mth,
        m.transaction_count_mth,
        m.revenue_mth,
        m.late_fee_mth,
        m.servfee_mth,
        m.delivery_fee_mth,
        m.oth_card_fee_mth,
        m.reactive_fee_mth,
        m.rebate_mth,
        m.edgefuel_mth,
        m.interest_mth,
        m.revshare_mth,
        m.pmf_mth,
        m.disrev_mth,
        m.total_fee_mth,
        m.sr_count_mth,
        m.sr_closed_mth,
        m.case_total_count,
        m.case_customer_current_count,
        m.case_null_count,
        m.case_account_count,
        m.case_payment_billing_count,
        m.case_online_ivr_ma_count,
        m.case_card_maintenance_count,
        m.case_fraud_count,
        m.case_card_declined_count,
        m.case_fee_waiver_count,
        m.case_junk_email_count,
        m.case_payment_billing_rec_count,
        m.case_termination_non_vas_count,
        m.case_authorization_count,
        m.case_application_fraud_count,
        m.case_claims_count,
        m.case_fees_count,
        m.case_disputes_count,
        m.case_other_count,
        m.is_active_month,
        m.cr_limit_mth,
        m.outstanding_balance_mth,

        -- ── AGING / CREDIT ──
        m.WX_DPD_MTH,
        m.TOTAL_EXPOSURE_MTH,

        -- ── SBFE ──
        m.SBFE_hit_flag,
        m.SBFEACCOUNT_COUNT,
        m.SBFEOPEN_LINE_COUNT,
        m.SBFEOPEN_CARD_COUNT,
        m.SBFERECENT_BALANCE_AMT,
        m.SBFECLOSED_COUNT,
        m.SBFECLOSED_INVOLUNTARY_COUNT,
        m.SBFEUTIL_CURRENT_REVOLVING,
        m.SBFE_acct_3m_ago,
        m.SBFE_opnline_3m_ago,
        m.SBFE_opncard_3m_ago,
        m.SBFE_bal_3m_ago,
        m.SBFE_closdcnt_3m_ago,
        m.SBFE_closd_involcnt_3m_ago,
        m.SBFE_util_3m_ago,
        m.SBFE_acct_6m_ago,
        m.SBFE_opnline_6m_ago,
        m.SBFE_opncard_6m_ago,
        m.SBFE_bal_6m_ago,
        m.SBFE_closdcnt_6m_ago,
        m.SBFE_closd_involcnt_6m_ago,
        m.SBFE_util_6m_ago,
        m.SBFE_acct_change_3m,
        m.SBFE_opnline_change_3m,
        m.SBFE_opncard_change_3m,
        m.SBFE_bal_change_3m,
        m.SBFE_closdcnt_change_3m,
        m.SBFE_closd_involcnt_change_3m,
        m.SBFE_util_change_3m,
        m.SBFE_acct_change_6m,
        m.SBFE_opnline_change_6m,
        m.SBFE_opncard_change_6m,
        m.SBFE_bal_change_6m,
        m.SBFE_closdcnt_change_6m,
        m.SBFE_closd_involcnt_change_6m,
        m.SBFE_util_change_6m,

        -- ── CLOSURE FLAGS ──
        m.closed_next_3m_flag,
        m.closed_next_6m_flag,
        m.closed_next_8m_flag,
        m.closed_next_12m_flag

    FROM WORKSPACE.digitalda_stage.Account_ID_features_monthly m
    LEFT JOIN WORKSPACE.digitalda_stage.entity_Acct_map_Christian c
        ON  c.ACCOUNTNUMBER         = m.account_id
        AND DATE_TRUNC('month', c.OBS_DATE) = m.cohort_month
),

-- ══════════════════════════════════════════════════════════════
-- STEP 2: Aggregate to entity × cohort_month
--         Produces raw entity-month totals (no rolling windows yet)
-- ══════════════════════════════════════════════════════════════
entity_month_raw AS (
    SELECT
        -- ── GRAIN ──
        ORG_URI,
        cohort_month,

        -- ── ACCOUNT COUNTS ──
        COUNT(DISTINCT account_id)                                              AS account_count,
        COUNT(DISTINCT CASE WHEN YEAR(account_closed_date) = 9999
                             OR account_closed_date IS NULL
                             THEN account_id END)                               AS active_account_count,

        -- ── SMALL BUSINESS COUNTS  ──
        -- small biz = outstanding cards < 10 at account level
        COUNT(DISTINCT CASE WHEN is_small_biz = 'Yes' THEN account_id END)     AS small_biz_account_count,
        COUNT(DISTINCT CASE WHEN is_small_biz = 'No'  THEN account_id END)     AS non_small_biz_account_count,

        -- ── ENTITY-LEVEL CARD/ACTIVITY COLUMNS (from C1 mapping) ──
        SUM(entity_outstanding_card_count)                                      AS total_outstanding_cards,
        SUM(entity_active_card_count)                                           AS total_active_cards,
        SUM(entity_purchase_gallons_qty)                                        AS total_entity_gallons,
        SUM(entity_wex_transaction_count)                                       AS total_entity_txns,
        AVG(entity_account_tenure_months)                                       AS avg_entity_tenure_months,
        MAX(entity_cr_limit)                                                    AS max_entity_cr_limit,
        MAX(entity_has_credit_risk)                                             AS has_any_credit_risk,

        -- ── STATIC / IDENTITY ──
        COUNT(DISTINCT CASE WHEN partner_ind = 'Wex'     THEN account_id END)  AS account_count_wex,
        COUNT(DISTINCT CASE WHEN partner_ind = 'Partner' THEN account_id END)  AS account_count_partner,
        MAX(is_vip_ind)                                                         AS has_vip_account,
        MAX(is_govt_ind)                                                        AS has_govt_account,
        MIN(account_open_date)                                                  AS earliest_account_open_date,
        MAX(account_open_date)                                                  AS latest_account_open_date,
        MAX(CASE WHEN YEAR(account_closed_date) < 3000 THEN account_closed_date ELSE NULL END) AS latest_account_closed_date,
        MIN(CASE WHEN YEAR(account_closed_date) < 3000 THEN account_closed_date ELSE NULL END) AS earliest_account_closed_date,
        AVG(tenure_months)                                                      AS avg_tenure_months,
        MAX(tenure_months)                                                      AS max_tenure_months,
        AVG(acquisition_fico)                                                   AS avg_acquisition_fico,
        MAX(acquisition_fico)                                                   AS max_acquisition_fico,
        AVG(credit_bureau_score)                                                AS avg_credit_bureau_score,
        MAX(credit_bureau_score)                                                AS max_credit_bureau_score,
        SUM(approved_credit_limit)                                              AS total_approved_credit_limit,
        MAX(approved_credit_limit)                                              AS max_approved_credit_limit,
        SUM(app_fleet_size)                                                     AS total_app_fleet_size,
        MAX(years_in_business)                                                  AS max_years_in_business,
        MAX(risk_grade)                                                         AS max_risk_grade,
        MAX(CASE WHEN is_fraud_ind = 'Fraud' THEN 1 ELSE 0 END)                AS fraud_account_appl_ind,
        SUM(CASE WHEN is_fraud_ind = 'Fraud' THEN 1 ELSE 0 END)                AS fraud_account_appl_count,
        SUM(CASE WHEN sales_type = 'Digital Marketing' THEN 1 ELSE 0 END)      AS Salestype_DM_count,
        SUM(CASE WHEN sales_type = 'Inside Sales'      THEN 1 ELSE 0 END)      AS Salestype_IS_count,
        SUM(CASE WHEN sales_type = 'Field Sales'       THEN 1 ELSE 0 END)      AS Salestype_FS_count,
        MAX(is_trucking_industry)                                               AS has_trucking_account,
        MIN(first_transaction_dt)                                               AS earliest_first_txn_dt,
        AVG(days_to_first_txn)                                                  AS avg_days_to_first_txn,

        -- ── FLEET ──
        SUM(max_active_cardcnt_mth)                                             AS total_active_cards_mth,
        SUM(outstanding_cardcnt_mth)                                            AS total_outstanding_cards_mth,

        -- ── CURRENT MONTH FLOW METRICS ──
        SUM(declined_txn_mth)                                                   AS declined_txn_mth,
        SUM(gross_spend_mth)                                                    AS gross_spend_mth,
        SUM(fuel_spend_mth)                                                     AS fuel_spend_mth,
        SUM(INN_spend_mth)                                                      AS INN_spend_mth,
        SUM(OON_spend_mth)                                                      AS OON_spend_mth,
        SUM(gallons_mth)                                                        AS gallons_mth,
        SUM(INN_gallons_mth)                                                    AS INN_gallons_mth,
        SUM(OON_gallons_mth)                                                    AS OON_gallons_mth,
        SUM(transaction_count_mth)                                              AS transaction_count_mth,
        SUM(revenue_mth)                                                        AS revenue_mth,
        SUM(late_fee_mth)                                                       AS late_fee_mth,
        SUM(servfee_mth)                                                        AS servfee_mth,
        SUM(delivery_fee_mth)                                                   AS delivery_fee_mth,
        SUM(oth_card_fee_mth)                                                   AS oth_card_fee_mth,
        SUM(reactive_fee_mth)                                                   AS reactive_fee_mth,
        SUM(rebate_mth)                                                         AS rebate_mth,
        SUM(edgefuel_mth)                                                       AS edgefuel_mth,
        SUM(interest_mth)                                                       AS interest_mth,
        SUM(revshare_mth)                                                       AS revshare_mth,
        SUM(pmf_mth)                                                            AS pmf_mth,
        SUM(disrev_mth)                                                         AS disrev_mth,
        SUM(total_fee_mth)                                                      AS total_fee_mth,
        SUM(sr_count_mth)                                                       AS sr_count_mth,
        SUM(sr_closed_mth)                                                      AS sr_closed_mth,
        SUM(case_total_count)                                                   AS case_total_count,
        SUM(case_customer_current_count)                                        AS case_customer_current_count,
        SUM(case_null_count)                                                    AS case_null_count,
        SUM(case_account_count)                                                 AS case_account_count,
        SUM(case_payment_billing_count)                                         AS case_payment_billing_count,
        SUM(case_online_ivr_ma_count)                                           AS case_online_ivr_ma_count,
        SUM(case_card_maintenance_count)                                        AS case_card_maintenance_count,
        SUM(case_fraud_count)                                                   AS case_fraud_count,
        SUM(case_card_declined_count)                                           AS case_card_declined_count,
        SUM(case_fee_waiver_count)                                              AS case_fee_waiver_count,
        SUM(case_junk_email_count)                                              AS case_junk_email_count,
        SUM(case_payment_billing_rec_count)                                     AS case_payment_billing_rec_count,
        SUM(case_termination_non_vas_count)                                     AS case_termination_non_vas_count,
        SUM(case_authorization_count)                                           AS case_authorization_count,
        SUM(case_application_fraud_count)                                       AS case_application_fraud_count,
        SUM(case_claims_count)                                                  AS case_claims_count,
        SUM(case_fees_count)                                                    AS case_fees_count,
        SUM(case_disputes_count)                                                AS case_disputes_count,
        SUM(case_other_count)                                                   AS case_other_count,
        MAX(is_active_month)                                                    AS entity_active_mth,
        MAX(cr_limit_mth)                                                       AS max_cr_limit_mth,
        SUM(cr_limit_mth)                                                       AS total_cr_limit_mth,
        SUM(outstanding_balance_mth)                                            AS total_outstanding_balance_mth,

        -- ── AGING / CREDIT ──
        MAX(WX_DPD_MTH)                                                         AS max_wx_dpd_mth,
        SUM(TOTAL_EXPOSURE_MTH)                                                 AS total_exposure_mth,

        -- ── SBFE (aggregated across accounts) ──
        MAX(SBFE_hit_flag)                                                      AS sbfe_any_hit,
        SUM(SBFEACCOUNT_COUNT)                                                  AS sum_sbfe_account_count,
        SUM(SBFEOPEN_LINE_COUNT)                                                AS sum_sbfe_open_line_count,
        SUM(SBFEOPEN_CARD_COUNT)                                                AS sum_sbfe_open_card_count,
        SUM(SBFERECENT_BALANCE_AMT)                                             AS sum_sbfe_recent_balance,
        SUM(SBFECLOSED_COUNT)                                                   AS sum_sbfe_closed_count,
        SUM(SBFECLOSED_INVOLUNTARY_COUNT)                                       AS sum_sbfe_closed_inv_count,
        SUM(SBFEUTIL_CURRENT_REVOLVING)                                         AS sum_sbfe_util_revolving,
        SUM(SBFE_acct_3m_ago)                                                   AS sum_sbfe_acct_3m_ago,
        SUM(SBFE_opnline_3m_ago)                                                AS sum_sbfe_opnline_3m_ago,
        SUM(SBFE_opncard_3m_ago)                                                AS sum_sbfe_opncard_3m_ago,
        SUM(SBFE_bal_3m_ago)                                                    AS sum_sbfe_bal_3m_ago,
        SUM(SBFE_closdcnt_3m_ago)                                               AS sum_sbfe_closdcnt_3m_ago,
        SUM(SBFE_closd_involcnt_3m_ago)                                         AS sum_sbfe_closd_involcnt_3m_ago,
        SUM(SBFE_util_3m_ago)                                                   AS sum_sbfe_util_3m_ago,
        SUM(SBFE_acct_6m_ago)                                                   AS sum_sbfe_acct_6m_ago,
        SUM(SBFE_opnline_6m_ago)                                                AS sum_sbfe_opnline_6m_ago,
        SUM(SBFE_opncard_6m_ago)                                                AS sum_sbfe_opncard_6m_ago,
        SUM(SBFE_bal_6m_ago)                                                    AS sum_sbfe_bal_6m_ago,
        SUM(SBFE_closdcnt_6m_ago)                                               AS sum_sbfe_closdcnt_6m_ago,
        SUM(SBFE_closd_involcnt_6m_ago)                                         AS sum_sbfe_closd_involcnt_6m_ago,
        SUM(SBFE_util_6m_ago)                                                   AS sum_sbfe_util_6m_ago,
        SUM(SBFE_acct_change_3m)                                                AS sum_sbfe_acct_change_3m,
        SUM(SBFE_opnline_change_3m)                                             AS sum_sbfe_opnline_change_3m,
        SUM(SBFE_opncard_change_3m)                                             AS sum_sbfe_opncard_change_3m,
        SUM(SBFE_bal_change_3m)                                                 AS sum_sbfe_bal_change_3m,
        SUM(SBFE_closdcnt_change_3m)                                            AS sum_sbfe_closdcnt_change_3m,
        SUM(SBFE_closd_involcnt_change_3m)                                      AS sum_sbfe_closd_involcnt_change_3m,
        SUM(SBFE_util_change_3m)                                                AS sum_sbfe_util_change_3m,
        SUM(SBFE_acct_change_6m)                                                AS sum_sbfe_acct_change_6m,
        SUM(SBFE_opnline_change_6m)                                             AS sum_sbfe_opnline_change_6m,
        SUM(SBFE_opncard_change_6m)                                             AS sum_sbfe_opncard_change_6m,
        SUM(SBFE_bal_change_6m)                                                 AS sum_sbfe_bal_change_6m,
        SUM(SBFE_closdcnt_change_6m)                                            AS sum_sbfe_closdcnt_change_6m,
        SUM(SBFE_closd_involcnt_change_6m)                                      AS sum_sbfe_closd_involcnt_change_6m,
        SUM(SBFE_util_change_6m)                                                AS sum_sbfe_util_change_6m,

        -- ── CLOSURE FLAGS (entity flags: 1 if ANY account closes in window) ──
        MAX(closed_next_3m_flag)                                                AS any_closed_next_3m,
        MAX(closed_next_6m_flag)                                                AS any_closed_next_6m,
        MAX(closed_next_8m_flag)                                                AS any_closed_next_8m,
        MAX(closed_next_12m_flag)                                               AS any_closed_next_12m

    FROM merged
    GROUP BY ORG_URI, cohort_month
),

-- ══════════════════════════════════════════════════════════════
-- STEP 3: Rolling window aggregations on ENTITY-month totals
--         Windows run on entity_month_raw, not on account-level pre-calcs.
--         ROWS BETWEEN N PRECEDING AND 1 PRECEDING = strictly prior N months.
-- ══════════════════════════════════════════════════════════════
entity_rolling AS (
    SELECT
        ORG_URI,
        cohort_month,

        -- ─────────────────────────────────────────────────
        -- L1M  (prior month value)
        -- ─────────────────────────────────────────────────
        LAG(gross_spend_mth, 1)         OVER (PARTITION BY ORG_URI ORDER BY cohort_month) AS spend_l1m,
        LAG(fuel_spend_mth, 1)          OVER (PARTITION BY ORG_URI ORDER BY cohort_month) AS fuel_spend_l1m,
        LAG(gallons_mth, 1)             OVER (PARTITION BY ORG_URI ORDER BY cohort_month) AS gallons_l1m,
        LAG(revenue_mth, 1)             OVER (PARTITION BY ORG_URI ORDER BY cohort_month) AS revenue_l1m,
        LAG(transaction_count_mth, 1)   OVER (PARTITION BY ORG_URI ORDER BY cohort_month) AS transactions_l1m,
        LAG(declined_txn_mth, 1)        OVER (PARTITION BY ORG_URI ORDER BY cohort_month) AS declined_txn_l1m,
        LAG(total_fee_mth, 1)           OVER (PARTITION BY ORG_URI ORDER BY cohort_month) AS total_fee_l1m,
        LAG(late_fee_mth, 1)            OVER (PARTITION BY ORG_URI ORDER BY cohort_month) AS late_fee_l1m,
        LAG(servfee_mth, 1)             OVER (PARTITION BY ORG_URI ORDER BY cohort_month) AS service_fee_l1m,
        LAG(delivery_fee_mth, 1)        OVER (PARTITION BY ORG_URI ORDER BY cohort_month) AS delivery_fee_l1m,
        LAG(oth_card_fee_mth, 1)        OVER (PARTITION BY ORG_URI ORDER BY cohort_month) AS other_card_fee_l1m,
        LAG(reactive_fee_mth, 1)        OVER (PARTITION BY ORG_URI ORDER BY cohort_month) AS reactivation_fee_l1m,
        LAG(sr_count_mth, 1)            OVER (PARTITION BY ORG_URI ORDER BY cohort_month) AS sr_total_l1m,
        LAG(sr_closed_mth, 1)           OVER (PARTITION BY ORG_URI ORDER BY cohort_month) AS sr_closed_l1m,
        LAG(case_total_count, 1)        OVER (PARTITION BY ORG_URI ORDER BY cohort_month) AS case_total_l1m,
        LAG(total_cr_limit_mth, 1)      OVER (PARTITION BY ORG_URI ORDER BY cohort_month) AS total_cr_limit_l1m,
        LAG(max_cr_limit_mth, 1)        OVER (PARTITION BY ORG_URI ORDER BY cohort_month) AS max_cr_limit_l1m,
        LAG(total_outstanding_balance_mth,1) OVER (PARTITION BY ORG_URI ORDER BY cohort_month) AS avg_outstanding_balance_l1m,
        LAG(entity_active_mth, 1)       OVER (PARTITION BY ORG_URI ORDER BY cohort_month) AS active_months_l1m,

        -- ─────────────────────────────────────────────────
        -- L3M  SUM
        -- ─────────────────────────────────────────────────
        SUM(gross_spend_mth)        OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS spend_sum_l3m,
        SUM(fuel_spend_mth)         OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS fuel_spend_sum_l3m,
        SUM(gallons_mth)            OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS gallons_sum_l3m,
        SUM(revenue_mth)            OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS revenue_sum_l3m,
        SUM(transaction_count_mth)  OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS transactions_sum_l3m,
        SUM(declined_txn_mth)       OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS declined_txn_sum_l3m,
        SUM(total_fee_mth)          OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS total_fee_sum_l3m,
        SUM(late_fee_mth)           OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS late_fee_sum_l3m,
        SUM(servfee_mth)            OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS service_fee_sum_l3m,
        SUM(delivery_fee_mth)       OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS delivery_fee_sum_l3m,
        SUM(oth_card_fee_mth)       OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS other_card_fee_sum_l3m,
        SUM(reactive_fee_mth)       OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS reactivation_fee_sum_l3m,
        SUM(sr_count_mth)           OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS sr_total_sum_l3m,
        SUM(sr_closed_mth)          OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS sr_closed_sum_l3m,
        SUM(case_total_count)                   OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS case_total_sum_l3m,
        SUM(case_customer_current_count)        OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS case_customer_current_sum_l3m,
        SUM(case_null_count)                    OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS case_null_sum_l3m,
        SUM(case_account_count)                 OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS case_account_sum_l3m,
        SUM(case_payment_billing_count)         OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS case_payment_billing_sum_l3m,
        SUM(case_online_ivr_ma_count)           OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS case_online_ivr_ma_sum_l3m,
        SUM(case_card_maintenance_count)        OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS case_card_maintenance_sum_l3m,
        SUM(case_fraud_count)                   OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS case_fraud_sum_l3m,
        SUM(case_card_declined_count)           OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS case_card_declined_sum_l3m,
        SUM(case_fee_waiver_count)              OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS case_fee_waiver_sum_l3m,
        SUM(case_junk_email_count)              OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS case_junk_email_sum_l3m,
        SUM(case_payment_billing_rec_count)     OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS case_payment_billing_rec_sum_l3m,
        SUM(case_termination_non_vas_count)     OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS case_termination_non_vas_sum_l3m,
        SUM(case_authorization_count)           OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS case_authorization_sum_l3m,
        SUM(case_application_fraud_count)       OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS case_application_fraud_sum_l3m,
        SUM(case_claims_count)                  OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS case_claims_sum_l3m,
        SUM(case_fees_count)                    OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS case_fees_sum_l3m,
        SUM(case_disputes_count)                OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS case_disputes_sum_l3m,
        SUM(case_other_count)                   OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS case_other_sum_l3m,
        SUM(entity_active_mth)      OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS active_months_l3m,
        -- L3M AVG
        AVG(gross_spend_mth)        OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS spend_avg_l3m,
        AVG(fuel_spend_mth)         OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS fuel_spend_avg_l3m,
        AVG(gallons_mth)            OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS gallons_avg_l3m,
        AVG(revenue_mth)            OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS revenue_avg_l3m,
        AVG(transaction_count_mth)  OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS transactions_avg_l3m,
        AVG(total_fee_mth)          OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS total_fee_avg_l3m,
        AVG(sr_count_mth)           OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS sr_total_avg_l3m,
        MAX(total_cr_limit_mth)     OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS max_cr_limit_l3m,
        MIN(total_cr_limit_mth)     OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS min_cr_limit_l3m,
        AVG(total_outstanding_balance_mth) OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS avg_outstanding_balance_l3m,

        -- ─────────────────────────────────────────────────
        -- L6M  SUM
        -- ─────────────────────────────────────────────────
        SUM(gross_spend_mth)        OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS spend_sum_l6m,
        SUM(fuel_spend_mth)         OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS fuel_spend_sum_l6m,
        SUM(gallons_mth)            OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS gallons_sum_l6m,
        SUM(revenue_mth)            OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS revenue_sum_l6m,
        SUM(transaction_count_mth)  OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS transactions_sum_l6m,
        SUM(declined_txn_mth)       OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS declined_txn_sum_l6m,
        SUM(total_fee_mth)          OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS total_fee_sum_l6m,
        SUM(late_fee_mth)           OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS late_fee_sum_l6m,
        SUM(servfee_mth)            OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS service_fee_sum_l6m,
        SUM(delivery_fee_mth)       OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS delivery_fee_sum_l6m,
        SUM(oth_card_fee_mth)       OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS other_card_fee_sum_l6m,
        SUM(reactive_fee_mth)       OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS reactivation_fee_sum_l6m,
        SUM(sr_count_mth)           OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS sr_total_sum_l6m,
        SUM(sr_closed_mth)          OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS sr_closed_sum_l6m,
        SUM(case_total_count)                   OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS case_total_sum_l6m,
        SUM(case_customer_current_count)        OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS case_customer_current_sum_l6m,
        SUM(case_null_count)                    OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS case_null_sum_l6m,
        SUM(case_account_count)                 OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS case_account_sum_l6m,
        SUM(case_payment_billing_count)         OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS case_payment_billing_sum_l6m,
        SUM(case_online_ivr_ma_count)           OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS case_online_ivr_ma_sum_l6m,
        SUM(case_card_maintenance_count)        OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS case_card_maintenance_sum_l6m,
        SUM(case_fraud_count)                   OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS case_fraud_sum_l6m,
        SUM(case_card_declined_count)           OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS case_card_declined_sum_l6m,
        SUM(case_fee_waiver_count)              OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS case_fee_waiver_sum_l6m,
        SUM(case_junk_email_count)              OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS case_junk_email_sum_l6m,
        SUM(case_payment_billing_rec_count)     OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS case_payment_billing_rec_sum_l6m,
        SUM(case_termination_non_vas_count)     OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS case_termination_non_vas_sum_l6m,
        SUM(case_authorization_count)           OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS case_authorization_sum_l6m,
        SUM(case_application_fraud_count)       OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS case_application_fraud_sum_l6m,
        SUM(case_claims_count)                  OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS case_claims_sum_l6m,
        SUM(case_fees_count)                    OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS case_fees_sum_l6m,
        SUM(case_disputes_count)                OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS case_disputes_sum_l6m,
        SUM(case_other_count)                   OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS case_other_sum_l6m,
        SUM(entity_active_mth)      OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS active_months_l6m,
        -- L6M AVG
        AVG(gross_spend_mth)        OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS spend_avg_l6m,
        AVG(fuel_spend_mth)         OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS fuel_spend_avg_l6m,
        AVG(gallons_mth)            OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS gallons_avg_l6m,
        AVG(revenue_mth)            OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS revenue_avg_l6m,
        AVG(transaction_count_mth)  OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS transactions_avg_l6m,
        AVG(total_fee_mth)          OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS total_fee_avg_l6m,
        AVG(sr_count_mth)           OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS sr_total_avg_l6m,
        MAX(total_cr_limit_mth)     OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS max_cr_limit_l6m,
        MIN(total_cr_limit_mth)     OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS min_cr_limit_l6m,
        AVG(total_outstanding_balance_mth) OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS avg_outstanding_balance_l6m,

        -- ─────────────────────────────────────────────────
        -- L12M  SUM
        -- ─────────────────────────────────────────────────
        SUM(gross_spend_mth)        OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS spend_sum_l12m,
        SUM(fuel_spend_mth)         OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS fuel_spend_sum_l12m,
        SUM(gallons_mth)            OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS gallons_sum_l12m,
        SUM(revenue_mth)            OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS revenue_sum_l12m,
        SUM(transaction_count_mth)  OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS transactions_sum_l12m,
        SUM(declined_txn_mth)       OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS declined_txn_sum_l12m,
        SUM(total_fee_mth)          OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS total_fee_sum_l12m,
        SUM(late_fee_mth)           OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS late_fee_sum_l12m,
        SUM(servfee_mth)            OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS service_fee_sum_l12m,
        SUM(delivery_fee_mth)       OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS delivery_fee_sum_l12m,
        SUM(oth_card_fee_mth)       OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS other_card_fee_sum_l12m,
        SUM(reactive_fee_mth)       OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS reactivation_fee_sum_l12m,
        SUM(sr_count_mth)           OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS sr_total_sum_l12m,
        SUM(sr_closed_mth)          OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS sr_closed_sum_l12m,
        SUM(case_total_count)                   OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS case_total_sum_l12m,
        SUM(case_customer_current_count)        OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS case_customer_current_sum_l12m,
        SUM(case_null_count)                    OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS case_null_sum_l12m,
        SUM(case_account_count)                 OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS case_account_sum_l12m,
        SUM(case_payment_billing_count)         OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS case_payment_billing_sum_l12m,
        SUM(case_online_ivr_ma_count)           OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS case_online_ivr_ma_sum_l12m,
        SUM(case_card_maintenance_count)        OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS case_card_maintenance_sum_l12m,
        SUM(case_fraud_count)                   OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS case_fraud_sum_l12m,
        SUM(case_card_declined_count)           OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS case_card_declined_sum_l12m,
        SUM(case_fee_waiver_count)              OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS case_fee_waiver_sum_l12m,
        SUM(case_junk_email_count)              OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS case_junk_email_sum_l12m,
        SUM(case_payment_billing_rec_count)     OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS case_payment_billing_rec_sum_l12m,
        SUM(case_termination_non_vas_count)     OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS case_termination_non_vas_sum_l12m,
        SUM(case_authorization_count)           OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS case_authorization_sum_l12m,
        SUM(case_application_fraud_count)       OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS case_application_fraud_sum_l12m,
        SUM(case_claims_count)                  OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS case_claims_sum_l12m,
        SUM(case_fees_count)                    OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS case_fees_sum_l12m,
        SUM(case_disputes_count)                OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS case_disputes_sum_l12m,
        SUM(case_other_count)                   OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS case_other_sum_l12m,
        SUM(entity_active_mth)      OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS active_months_l12m,
        -- L12M AVG
        AVG(gross_spend_mth)        OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS spend_avg_l12m,
        AVG(fuel_spend_mth)         OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS fuel_spend_avg_l12m,
        AVG(gallons_mth)            OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS gallons_avg_l12m,
        AVG(revenue_mth)            OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS revenue_avg_l12m,
        AVG(transaction_count_mth)  OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS transactions_avg_l12m,
        AVG(total_fee_mth)          OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS total_fee_avg_l12m,
        AVG(sr_count_mth)           OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS sr_total_avg_l12m,
        MAX(total_cr_limit_mth)     OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS max_cr_limit_l12m,
        MIN(total_cr_limit_mth)     OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS min_cr_limit_l12m,
        AVG(total_outstanding_balance_mth) OVER (PARTITION BY ORG_URI ORDER BY cohort_month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS avg_outstanding_balance_l12m

    FROM entity_month_raw
    
)

-- ══════════════════════════════════════════════════════════════
-- FINAL: Join raw entity snapshot with rolling features
-- ══════════════════════════════════════════════════════════════
SELECT
    r.*,

    -- ── ROLLING L1M ──
    er.spend_l1m,            er.fuel_spend_l1m,        er.gallons_l1m,
    er.revenue_l1m,          er.transactions_l1m,       er.declined_txn_l1m,
    er.total_fee_l1m,        er.late_fee_l1m,           er.service_fee_l1m,
    er.delivery_fee_l1m,     er.other_card_fee_l1m,     er.reactivation_fee_l1m,
    er.sr_total_l1m,         er.sr_closed_l1m,          er.case_total_l1m,
    er.total_cr_limit_l1m,   er.max_cr_limit_l1m,       er.avg_outstanding_balance_l1m,
    er.active_months_l1m,    

    -- ── ROLLING L3M ──
    er.spend_sum_l3m,        er.fuel_spend_sum_l3m,     er.gallons_sum_l3m,
    er.revenue_sum_l3m,      er.transactions_sum_l3m,   er.declined_txn_sum_l3m,
    er.total_fee_sum_l3m,    er.late_fee_sum_l3m,       er.service_fee_sum_l3m,
    er.delivery_fee_sum_l3m, er.other_card_fee_sum_l3m, er.reactivation_fee_sum_l3m,
    er.sr_total_sum_l3m,     er.sr_closed_sum_l3m,
    er.case_total_sum_l3m,         er.case_customer_current_sum_l3m, er.case_null_sum_l3m,
    er.case_account_sum_l3m,       er.case_payment_billing_sum_l3m,  er.case_online_ivr_ma_sum_l3m,
    er.case_card_maintenance_sum_l3m, er.case_fraud_sum_l3m,         er.case_card_declined_sum_l3m,
    er.case_fee_waiver_sum_l3m,    er.case_junk_email_sum_l3m,       er.case_payment_billing_rec_sum_l3m,
    er.case_termination_non_vas_sum_l3m, er.case_authorization_sum_l3m, er.case_application_fraud_sum_l3m,
    er.case_claims_sum_l3m,        er.case_fees_sum_l3m,             er.case_disputes_sum_l3m,
    er.case_other_sum_l3m,
    er.active_months_l3m,
    er.spend_avg_l3m,        er.fuel_spend_avg_l3m,     er.gallons_avg_l3m,
    er.revenue_avg_l3m,      er.transactions_avg_l3m,   er.total_fee_avg_l3m,
    er.sr_total_avg_l3m,     er.max_cr_limit_l3m,       er.min_cr_limit_l3m,    er.avg_outstanding_balance_l3m,
    

    -- ── ROLLING L6M ──
    er.spend_sum_l6m,        er.fuel_spend_sum_l6m,     er.gallons_sum_l6m,
    er.revenue_sum_l6m,      er.transactions_sum_l6m,   er.declined_txn_sum_l6m,
    er.total_fee_sum_l6m,    er.late_fee_sum_l6m,       er.service_fee_sum_l6m,
    er.delivery_fee_sum_l6m, er.other_card_fee_sum_l6m, er.reactivation_fee_sum_l6m,
    er.sr_total_sum_l6m,     er.sr_closed_sum_l6m,
    er.case_total_sum_l6m,         er.case_customer_current_sum_l6m, er.case_null_sum_l6m,
    er.case_account_sum_l6m,       er.case_payment_billing_sum_l6m,  er.case_online_ivr_ma_sum_l6m,
    er.case_card_maintenance_sum_l6m, er.case_fraud_sum_l6m,         er.case_card_declined_sum_l6m,
    er.case_fee_waiver_sum_l6m,    er.case_junk_email_sum_l6m,       er.case_payment_billing_rec_sum_l6m,
    er.case_termination_non_vas_sum_l6m, er.case_authorization_sum_l6m, er.case_application_fraud_sum_l6m,
    er.case_claims_sum_l6m,        er.case_fees_sum_l6m,             er.case_disputes_sum_l6m,
    er.case_other_sum_l6m,
    er.active_months_l6m,
    er.spend_avg_l6m,        er.fuel_spend_avg_l6m,     er.gallons_avg_l6m,
    er.revenue_avg_l6m,      er.transactions_avg_l6m,   er.total_fee_avg_l6m,
    er.sr_total_avg_l6m,     er.max_cr_limit_l6m,        er.min_cr_limit_l6m,       er.avg_outstanding_balance_l6m,

    -- ── ROLLING L12M ──
    er.spend_sum_l12m,       er.fuel_spend_sum_l12m,    er.gallons_sum_l12m,
    er.revenue_sum_l12m,     er.transactions_sum_l12m,  er.declined_txn_sum_l12m,
    er.total_fee_sum_l12m,   er.late_fee_sum_l12m,      er.service_fee_sum_l12m,
    er.delivery_fee_sum_l12m, er.other_card_fee_sum_l12m, er.reactivation_fee_sum_l12m,
    er.sr_total_sum_l12m,    er.sr_closed_sum_l12m,
    er.case_total_sum_l12m,         er.case_customer_current_sum_l12m, er.case_null_sum_l12m,
    er.case_account_sum_l12m,       er.case_payment_billing_sum_l12m,  er.case_online_ivr_ma_sum_l12m,
    er.case_card_maintenance_sum_l12m, er.case_fraud_sum_l12m,         er.case_card_declined_sum_l12m,
    er.case_fee_waiver_sum_l12m,    er.case_junk_email_sum_l12m,       er.case_payment_billing_rec_sum_l12m,
    er.case_termination_non_vas_sum_l12m, er.case_authorization_sum_l12m, er.case_application_fraud_sum_l12m,
    er.case_claims_sum_l12m,        er.case_fees_sum_l12m,             er.case_disputes_sum_l12m,
    er.case_other_sum_l12m,
    er.active_months_l12m,
    er.spend_avg_l12m,       er.fuel_spend_avg_l12m,    er.gallons_avg_l12m,
    er.revenue_avg_l12m,     er.transactions_avg_l12m,  er.total_fee_avg_l12m,
    er.sr_total_avg_l12m,    er.max_cr_limit_l12m,      er.min_cr_limit_l12m,    er.avg_outstanding_balance_l12m

FROM entity_month_raw r
LEFT JOIN entity_rolling er
    ON  er.ORG_URI      = r.ORG_URI
    AND er.cohort_month = r.cohort_month

ORDER BY r.ORG_URI, r.cohort_month
;

-- Quick validation
SELECT * FROM WORKSPACE.digitalda_stage.Entity_ID_features_monthly LIMIT 1000;
