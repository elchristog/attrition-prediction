-- ════════════════════════════════════════════════════════════════════════════
-- C2 — ACCOUNT_ID_features_monthly
-- Grain : account_id × cohort_month
-- Purpose: Raw current-month metrics per account ONLY.
--          Rolling windows are intentionally removed from this layer.
--          They will be computed AFTER entity-level aggregation in C3.
-- Change log:
--   • Removed rolling_stats CTE and all L1/L3/L6/L12 window columns
--   • Added is_small_biz flag  (outstanding_cards < 10 → 'Yes', else 'No')
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE WORKSPACE.digitalda_stage.Account_ID_features_monthly AS

WITH date_spine AS (
    SELECT
        DATE_TRUNC('month', DATEADD(month, seq4(), '2023-01-01')) AS month_start
    FROM TABLE(GENERATOR(ROWCOUNT => 40))   -- Jan 2023 → ~Apr 2026
),

-- ══════════════════════════════════════════════════════════════
-- CTE 1: Account static characteristics (from Salesforce)
-- ══════════════════════════════════════════════════════════════
account_characteristics AS (
    SELECT
        a.wex_account_nbr__c                                            AS account_id,
        MAX(TRY_TO_NUMBER(a.PG_FICO_SCORE__C::VARCHAR))                 AS acquisition_fico,
        MAX(TRY_TO_NUMBER(a.credit_bureau_score__c::VARCHAR))           AS credit_bureau_score,
        MAX(TRY_TO_NUMBER(a.approved_credit_limit__c::VARCHAR))         AS approved_credit_limit,
        MAX(TRY_TO_NUMBER(a.fleet_size__c::VARCHAR))                    AS app_fleet_size,
        MAX(TRY_TO_NUMBER(a.years_in_business__c::VARCHAR))             AS years_in_business,
        MAX(NULLIF(TRIM(a.risk_grade__c), ''))                          AS risk_grade,
        MAX(CASE
            WHEN a.risk_grade__c IN ('1','2','3') THEN 'RG 1-3'
            WHEN a.risk_grade__c  = '4'           THEN 'RG 4'
            WHEN a.risk_grade__c IN ('5','6','7') THEN 'RG 5-7'
            ELSE 'No RG'
        END)                                                            AS risk_grade_bucket,
        MAX(TRIM(CASE WHEN a.campaign_type__c IS NULL THEN b.campaign_type ELSE a.campaign_type__c END)) AS campaign_type_sf,
        MAX(TRIM(a.fraud_decision__c))                                  AS fraud_decision,
        MAX(TRIM(b.sales_type))                                         AS sales_type,
        MAX(TRIM(b.campaign_tactic))                                    AS campaign_tactic,
        MAX(CASE WHEN a.fraud_decision__c = 'Declined' OR a.fraud_flag__c = TRUE THEN 'Fraud' ELSE 'Not Fraud' END) AS is_fraud_ind
    FROM prep.salesforce_owner.application_request__c a
    LEFT JOIN (
        SELECT SOURCE_ACCOUNT_ID,
               MAX(sales_type)       AS sales_type,
               MAX(campaign_type)    AS campaign_type,
               MAX(campaign_tactic)  AS campaign_tactic
        FROM WORKSPACE.SALESMKTG.ACQ3_ACCOUNT
        GROUP BY ALL
    ) b ON a.wex_account_nbr__c = b.SOURCE_ACCOUNT_ID
    WHERE a.wex_account_nbr__c IS NOT NULL
    GROUP BY a.wex_account_nbr__c
),

-- ══════════════════════════════════════════════════════════════
-- CTE 2: NAICS description
-- ══════════════════════════════════════════════════════════════
account_metrics AS (
    SELECT
        account_id,
        MAX(naics_description)    AS naics_description,
        MAX(naics_industry_code)  AS naics_industry_code
    FROM finance_analytics.nam_portfolio_metrics.nam_account_metrics_view
    GROUP BY account_id
),

monthly_metrics AS (
    SELECT
        account_id,
        DATE_TRUNC('month', revenue_date)                    AS metric_month,
        SUM(EDW_NAF_GALLONS)                                 AS gallons_mth,
        SUM(INN_PURCHASE_GALLONS_QTY)                        AS INN_gallons_mth,
        SUM(EDW_NAF_GALLONS) - SUM(INN_PURCHASE_GALLONS_QTY) AS OON_gallons_mth,
        SUM(spend_fuel_only_amount)                          AS fuel_spend_mth,
        SUM(gross_spend_amount)                              AS gross_spend_mth,
        SUM(gross_spend_amount) - SUM(OON_GROSS_SPEND_AMOUNT) AS INN_spend_mth,
        SUM(OON_GROSS_SPEND_AMOUNT)                          AS OON_spend_mth,
        SUM(transaction_count)                               AS transaction_count_mth
    FROM finance_analytics.nam_portfolio_metrics.nam_account_metrics_view
    GROUP BY account_id, DATE_TRUNC('month', revenue_date)
),

revenue_monthly AS (
    SELECT account_id,
           calendar_date,
           SUM(revenue_amount_usd) AS revenue_amount_usd
    FROM finance_analytics.nam_portfolio_metrics.f_nam_monthly_revenue
    GROUP BY ALL
),

fees_monthly AS (
    SELECT
        a.account_id,
        DATE_TRUNC('month', a.revenue_date) AS fee_month,
        SUM(CASE WHEN rev_lvl2 ILIKE '%Delivery Fee%'     THEN REVENUE_AMOUNT_USD END) AS delivery_fee_mth,
        SUM(CASE WHEN rev_lvl2 ILIKE '%Other Card Fee%'   THEN REVENUE_AMOUNT_USD END) AS oth_card_fee_mth,
        SUM(CASE WHEN rev_lvl2 ILIKE '%Rebate%'           THEN REVENUE_AMOUNT_USD END) AS rebate_mth,
        SUM(CASE WHEN rev_lvl2 ILIKE '%EDGE Fuel%'        THEN REVENUE_AMOUNT_USD END) AS edgefuel_mth,
        SUM(CASE WHEN rev_lvl2 ILIKE '%Interest%'         THEN REVENUE_AMOUNT_USD END) AS interest_mth,
        SUM(CASE WHEN rev_lvl2 ILIKE '%Late Fees%'        THEN REVENUE_AMOUNT_USD END) AS late_fee_mth,
        SUM(CASE WHEN rev_lvl2 ILIKE '%Reactivation Fee%' THEN REVENUE_AMOUNT_USD END) AS reactive_fee_mth,
        SUM(CASE WHEN rev_lvl2 ILIKE '%Revenue Share%'    THEN REVENUE_AMOUNT_USD END) AS revshare_mth,
        SUM(CASE WHEN rev_lvl2 ILIKE '%Service Fee%'      THEN REVENUE_AMOUNT_USD END) AS servfee_mth,
        SUM(CASE WHEN rev_lvl2 ILIKE '%PMF%'              THEN REVENUE_AMOUNT_USD END) AS pmf_mth,
        SUM(CASE WHEN rev_lvl2 ILIKE '%Discount Revenue%' THEN REVENUE_AMOUNT_USD END) AS disrev_mth
    FROM FINANCE_ANALYTICS.NAM_PORTFOLIO_METRICS.nam_revenue_detailed_view a
    LEFT JOIN WORKSPACE.DIGITALDA.revcode_map b ON a.Revenue_code = b.Revenue_code
    GROUP BY account_id, DATE_TRUNC('month', a.revenue_date)
),

aging_metrics AS (
    SELECT
        CUST_ID,
        a.VOL_MONTH                                                         AS MTH_YR,
        MAX(CR_LIMIT)                                                       AS CR_LIMIT_MTH,
        MAX(WX_DAYS_PAST_DUE)                                               AS WX_DPD_MTH,
        SUM(COALESCE(WX_AGE99, 0) + COALESCE(WX_EIPP_BALANCE, 0))          AS TOTAL_EXPOSURE_MTH,
        MAX(COALESCE(b.outstanding_balance, 0))                             AS OUTSTANDING_BALANCE
    FROM (
        SELECT CUST_ID,
               DATE_TRUNC('month', BUSINESS_DATE) AS VOL_MONTH,
               WX_DAYS_PAST_DUE, WX_AGE99, WX_EIPP_BALANCE, WX_RCRSE_CODE, CR_LIMIT
        FROM (
            SELECT CUST_ID, BUSINESS_DATE, WX_DAYS_PAST_DUE, WX_AGE99, WX_EIPP_BALANCE, WX_RCRSE_CODE, CR_LIMIT
            FROM PREP.FIN__SYSADM.PS_WX_CUST_DAILY
            UNION ALL
            SELECT CUST_ID, BUSINESS_DATE, WX_DAYS_PAST_DUE, WX_AGE99, WX_EIPP_BALANCE, WX_RCRSE_CODE, CR_LIMIT
            FROM PREP.FIN__SYSADM_ARCH.PS_WX_CSTDAY_ARCH
            UNION ALL
            SELECT CUST_ID, BUSINESS_DATE, WX_DAYS_PAST_DUE, WX_AGE99, WX_EIPP_BALANCE, WX_RCRSE_CODE, CR_LIMIT
            FROM PREP.FIN__SYSADM_ARCH.PS_WX_CUST_DAILY_ARCH
        )
        QUALIFY RANK() OVER (PARTITION BY CUST_ID, DATE_TRUNC('month', BUSINESS_DATE) ORDER BY BUSINESS_DATE DESC) = 1
    ) a
    LEFT JOIN (
        SELECT DISTINCT acct_number,
                        DATE_TRUNC('month', business_date) AS vol_month,
                        MAX(ar_total_amt) AS outstanding_balance
        FROM (
            SELECT * FROM prep.fin__sysadm.ps_wx_rp13_summary
            QUALIFY RANK() OVER (PARTITION BY acct_number, DATE_TRUNC('month', business_date) ORDER BY business_date DESC) = 1
        )
        GROUP BY ALL
    ) b ON a.cust_id = b.acct_number AND a.vol_month = b.vol_month
    GROUP BY ALL
),

naics_mapping AS (
    SELECT account_id,
           CASE naics_2digit_code
               WHEN 11 THEN 'Yes'  -- Agriculture
               WHEN 21 THEN 'Yes'  -- Mining
               WHEN 23 THEN 'Yes'  -- Construction
               WHEN 31 THEN 'Yes'  -- Manufacturing
               WHEN 32 THEN 'Yes'  -- Manufacturing
               WHEN 33 THEN 'Yes'  -- Manufacturing
               WHEN 42 THEN 'Yes'  -- Wholesale Trade
               WHEN 48 THEN 'Yes'  -- Transportation & Warehousing
               WHEN 49 THEN 'Yes'  -- Transportation & Warehousing
               ELSE 'No'
           END AS is_trucking_industry
    FROM (
        SELECT *, CAST(LEFT(CAST(NAICS_INDUSTRY_CODE AS VARCHAR), 2) AS INTEGER) AS naics_2digit_code
        FROM account_metrics
    )
),

-- ══════════════════════════════════════════════════════════════
-- CTE 3: Fleet size monthly snapshot
-- ══════════════════════════════════════════════════════════════
fleet_snapshot_mthly AS (
    SELECT
        f.account_id,
        DATE_TRUNC('month', f.calendar_date) AS mth_yr,
        MAX(f.max_active_card_count)          AS max_active_cardcnt_mth,
        MAX(f.outstanding_card_count)         AS outstanding_cardcnt_mth
    FROM FINANCE_ANALYTICS.NAM_PORTFOLIO_METRICS.F_NAM_MONTHLY_ACCOUNT_METRICS f
    GROUP BY ALL
),

-- ══════════════════════════════════════════════════════════════
-- CTE 4: Service requests monthly
-- ══════════════════════════════════════════════════════════════
sr_monthly AS (
    SELECT
        c.wex_account__c               AS account_id,
        DATE_TRUNC('month', c.createddate) AS sr_month,
        COUNT(*)                       AS sr_count_mth,
        COUNT(CASE WHEN c.status = 'Closed' THEN 1 END) AS sr_closed_mth
    FROM prep.salesforce_owner.case c
    GROUP BY 1, 2
),

sr_monthly_casetypes AS (
    SELECT
        c.account_wex_account                                   AS account_id,
        DATE_TRUNC('month', c.case_created_date_utc)            AS case_month,
        COUNT(DISTINCT c.case_id)                               AS case_total_count,
        COUNT(DISTINCT CASE WHEN LOWER(c.case_primary_reason) = 'customer current'                THEN c.case_id END) AS case_customer_current_count,
        COUNT(DISTINCT CASE WHEN LOWER(COALESCE(c.case_primary_reason, 'null')) = 'null'           THEN c.case_id END) AS case_null_count,
        COUNT(DISTINCT CASE WHEN LOWER(c.case_primary_reason) = 'account'                         THEN c.case_id END) AS case_account_count,
        COUNT(DISTINCT CASE WHEN LOWER(c.case_primary_reason) = 'payment & billing'               THEN c.case_id END) AS case_payment_billing_count,
        COUNT(DISTINCT CASE WHEN LOWER(c.case_primary_reason) = 'online, ivr, & mobile assistance' THEN c.case_id END) AS case_online_ivr_ma_count,
        COUNT(DISTINCT CASE WHEN LOWER(c.case_primary_reason) = 'card maintenance'                THEN c.case_id END) AS case_card_maintenance_count,
        COUNT(DISTINCT CASE WHEN LOWER(c.case_primary_reason) = 'fraud'                           THEN c.case_id END) AS case_fraud_count,
        COUNT(DISTINCT CASE WHEN LOWER(c.case_primary_reason) = 'card declined'                   THEN c.case_id END) AS case_card_declined_count,
        COUNT(DISTINCT CASE WHEN LOWER(c.case_primary_reason) = 'fee waiver'                      THEN c.case_id END) AS case_fee_waiver_count,
        COUNT(DISTINCT CASE WHEN LOWER(c.case_primary_reason) = 'junk email'                      THEN c.case_id END) AS case_junk_email_count,
        COUNT(DISTINCT CASE WHEN LOWER(c.case_primary_reason) = 'payment & billing - rec'         THEN c.case_id END) AS case_payment_billing_rec_count,
        COUNT(DISTINCT CASE WHEN LOWER(c.case_primary_reason) = 'termination - non vas qualifying' THEN c.case_id END) AS case_termination_non_vas_count,
        COUNT(DISTINCT CASE WHEN LOWER(c.case_primary_reason) = 'authorization'                   THEN c.case_id END) AS case_authorization_count,
        COUNT(DISTINCT CASE WHEN LOWER(c.case_primary_reason) = 'application fraud'               THEN c.case_id END) AS case_application_fraud_count,
        COUNT(DISTINCT CASE WHEN LOWER(c.case_primary_reason) = 'claims'                          THEN c.case_id END) AS case_claims_count,
        COUNT(DISTINCT CASE WHEN LOWER(c.case_primary_reason) = 'fees'                            THEN c.case_id END) AS case_fees_count,
        COUNT(DISTINCT CASE WHEN LOWER(c.case_primary_reason) IN ('dispute', 'disputes')          THEN c.case_id END) AS case_disputes_count,
        COUNT(DISTINCT CASE WHEN LOWER(COALESCE(c.case_primary_reason, 'null')) NOT IN (
            'customer current','null','account','payment & billing','online, ivr, & mobile assistance',
            'card maintenance','fraud','card declined','fee waiver','junk email',
            'payment & billing - rec','termination - non vas qualifying','authorization',
            'application fraud','claims','fees','dispute','disputes'
        ) THEN case_id END)                                     AS case_other_count
    FROM global_fleet_analytics.callcenter.salesforce_case_details c
    GROUP BY ALL
),

-- ══════════════════════════════════════════════════════════════
-- CTE 5: Days to first transaction
-- ══════════════════════════════════════════════════════════════
first_transaction AS (
    SELECT
        r.account_id,
        MIN(TO_DATE(r.calendar_key::VARCHAR, 'YYYYMMDD')) AS first_transaction_dt
    FROM finance_analytics.nam_portfolio_metrics.f_nam_daily_account_metrics r
    WHERE r.purchase_gallons_qty > 0
    GROUP BY r.account_id
),

-- ══════════════════════════════════════════════════════════════
-- CTE 6: Declined transactions
-- ══════════════════════════════════════════════════════════════
declined_txn AS (
    SELECT
        c.source_account_id                              AS account_id,
        DATE_TRUNC('month', TO_DATE(c.loc_tran_dttm))   AS month_yr,
        COUNT(*)                                         AS declined_txn
    FROM GLOBAL_FLEET_ANALYTICS.EDW_OWNER.F_AUTH_HDR c
    WHERE c.decline_code NOT IN ('APPROVED', 'APPROVED_PARTIAL_AMOUNT')
    GROUP BY ALL
),

-- ══════════════════════════════════════════════════════════════
-- BASE DATA (accounts in scope)
-- ══════════════════════════════════════════════════════════════
base_data AS (
    SELECT DISTINCT
        dnam.account_id                                                          AS account_id,
        m.customer_id                                                            AS business_entity,
        m.business_program_name                                                  AS business_program_name,
        pt.partner                                                               AS partner,
        CASE WHEN UPPER(pt.partner) LIKE 'WEX%' THEN 'Wex' ELSE 'Partner' END  AS partner_ind,
        CASE WHEN vip.cust_id IS NOT NULL THEN 'VIP' ELSE 'Non-VIP' END        AS is_vip_ind,
        CASE WHEN m.is_government_account = FALSE THEN 'Non-Govt' ELSE 'Govt' END AS is_govt_ind,
        YEAR(dnam.account_open_date)                                             AS ACQUISITION_YEAR,
        MONTH(dnam.account_open_date)                                            AS ACQUISITION_MONTH,
        MAX(dnam.account_open_date)                                              AS account_open_date,
        MAX(dnam.account_closed_date)                                            AS account_closed_date,
        MAX(m.account_status)                                                    AS account_status
    FROM FINANCE_ANALYTICS.NAM_PORTFOLIO_METRICS.D_NAM_ACCOUNT dnam
    LEFT JOIN common.customer.d_account_master m ON m.account_number = dnam.account_id
    LEFT JOIN WORKSPACE.SALESMKTG.PROGRAM_TO_PARTNER_DS pt ON m.business_program_name = pt.edw_program_name
    LEFT JOIN (
        SELECT DISTINCT cust_id
        FROM prep.fin__sysadm.ps_wx_customer_wex
        WHERE CUST_ID != wx_national_id
          AND wx_cust_susp_class = 'V'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY cust_id ORDER BY date_last_maint DESC) = 1
    ) vip ON dnam.account_id = vip.cust_id
    WHERE (dnam.account_closed_date IS NULL
            OR YEAR(dnam.account_closed_date) = 9999
            OR YEAR(dnam.account_closed_date) >= 2023)
      AND YEAR(dnam.account_open_date) >= 1980
    GROUP BY ALL
),

-- ══════════════════════════════════════════════════════════════
-- ACCOUNT-MONTH SPINE
-- ══════════════════════════════════════════════════════════════
account_month_spine AS (
    SELECT
        b.account_id,
        d.month_start                   AS cohort_month,
        b.account_open_date,
        b.account_closed_date,
        b.account_status,
        b.business_entity,
        b.business_program_name,
        b.partner,
        b.partner_ind,
        b.is_vip_ind,
        b.is_govt_ind,
        b.ACQUISITION_YEAR,
        b.ACQUISITION_MONTH
    FROM base_data b
    CROSS JOIN date_spine d
    WHERE d.month_start >= DATE_TRUNC('month', b.account_open_date)
),

-- ══════════════════════════════════════════════════════════════
-- SBFE: Raw monthly bureau snapshot
-- ══════════════════════════════════════════════════════════════
sbfe_monthly AS (
    SELECT
        cust_id                                         AS account_id,
        DATE_TRUNC('month', _modified)                  AS bureau_month,
        SBFEACCOUNT_COUNT,
        SBFEOPEN_LINE_COUNT,
        SBFEOPEN_CARD_COUNT,
        SBFERECENT_BALANCE_AMT,
        SBFECLOSED_COUNT,
        SBFECLOSED_INVOLUNTARY_COUNT,
        SBFEUTIL_CURRENT_REVOLVING
    FROM prep.lexis_nexis.attr_and_scores
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY cust_id, DATE_TRUNC('month', _modified)
        ORDER BY _modified DESC
    ) = 1
),

-- ⚠ WEAK ASSUMPTION: bureau_month aligns with cohort_month on a calendar-month basis.
sbfe_features AS (
    SELECT
        s.account_id,
        s.cohort_month,
        CASE WHEN cur.SBFEACCOUNT_COUNT IS NOT NULL THEN 1 ELSE 0 END   AS SBFE_hit_flag,
        cur.SBFEACCOUNT_COUNT,
        cur.SBFEOPEN_LINE_COUNT,
        cur.SBFEOPEN_CARD_COUNT,
        cur.SBFERECENT_BALANCE_AMT,
        cur.SBFECLOSED_COUNT,
        cur.SBFECLOSED_INVOLUNTARY_COUNT,
        cur.SBFEUTIL_CURRENT_REVOLVING,
        -- 3-month lag values
        ago3.SBFEACCOUNT_COUNT          AS SBFE_acct_3m_ago,
        ago3.SBFEOPEN_LINE_COUNT        AS SBFE_opnline_3m_ago,
        ago3.SBFEOPEN_CARD_COUNT        AS SBFE_opncard_3m_ago,
        ago3.SBFERECENT_BALANCE_AMT     AS SBFE_bal_3m_ago,
        ago3.SBFECLOSED_COUNT           AS SBFE_closdcnt_3m_ago,
        ago3.SBFECLOSED_INVOLUNTARY_COUNT AS SBFE_closd_involcnt_3m_ago,
        ago3.SBFEUTIL_CURRENT_REVOLVING AS SBFE_util_3m_ago,
        -- 6-month lag values
        ago6.SBFEACCOUNT_COUNT          AS SBFE_acct_6m_ago,
        ago6.SBFEOPEN_LINE_COUNT        AS SBFE_opnline_6m_ago,
        ago6.SBFEOPEN_CARD_COUNT        AS SBFE_opncard_6m_ago,
        ago6.SBFERECENT_BALANCE_AMT     AS SBFE_bal_6m_ago,
        ago6.SBFECLOSED_COUNT           AS SBFE_closdcnt_6m_ago,
        ago6.SBFECLOSED_INVOLUNTARY_COUNT AS SBFE_closd_involcnt_6m_ago,
        ago6.SBFEUTIL_CURRENT_REVOLVING AS SBFE_util_6m_ago,
        -- 3-month changes
        COALESCE(cur.SBFEACCOUNT_COUNT, 0)          - COALESCE(ago3.SBFEACCOUNT_COUNT, 0)           AS SBFE_acct_change_3m,
        COALESCE(cur.SBFEOPEN_LINE_COUNT, 0)        - COALESCE(ago3.SBFEOPEN_LINE_COUNT, 0)         AS SBFE_opnline_change_3m,
        COALESCE(cur.SBFEOPEN_CARD_COUNT, 0)        - COALESCE(ago3.SBFEOPEN_CARD_COUNT, 0)         AS SBFE_opncard_change_3m,
        COALESCE(cur.SBFERECENT_BALANCE_AMT, 0)     - COALESCE(ago3.SBFERECENT_BALANCE_AMT, 0)      AS SBFE_bal_change_3m,
        COALESCE(cur.SBFECLOSED_COUNT, 0)           - COALESCE(ago3.SBFECLOSED_COUNT, 0)            AS SBFE_closdcnt_change_3m,
        COALESCE(cur.SBFECLOSED_INVOLUNTARY_COUNT,0)- COALESCE(ago3.SBFECLOSED_INVOLUNTARY_COUNT,0) AS SBFE_closd_involcnt_change_3m,
        COALESCE(cur.SBFEUTIL_CURRENT_REVOLVING, 0) - COALESCE(ago3.SBFEUTIL_CURRENT_REVOLVING, 0)  AS SBFE_util_change_3m,
        -- 6-month changes
        COALESCE(cur.SBFEACCOUNT_COUNT, 0)          - COALESCE(ago6.SBFEACCOUNT_COUNT, 0)           AS SBFE_acct_change_6m,
        COALESCE(cur.SBFEOPEN_LINE_COUNT, 0)        - COALESCE(ago6.SBFEOPEN_LINE_COUNT, 0)         AS SBFE_opnline_change_6m,
        COALESCE(cur.SBFEOPEN_CARD_COUNT, 0)        - COALESCE(ago6.SBFEOPEN_CARD_COUNT, 0)         AS SBFE_opncard_change_6m,
        COALESCE(cur.SBFERECENT_BALANCE_AMT, 0)     - COALESCE(ago6.SBFERECENT_BALANCE_AMT, 0)      AS SBFE_bal_change_6m,
        COALESCE(cur.SBFECLOSED_COUNT, 0)           - COALESCE(ago6.SBFECLOSED_COUNT, 0)            AS SBFE_closdcnt_change_6m,
        COALESCE(cur.SBFECLOSED_INVOLUNTARY_COUNT,0)- COALESCE(ago6.SBFECLOSED_INVOLUNTARY_COUNT,0) AS SBFE_closd_involcnt_change_6m,
        COALESCE(cur.SBFEUTIL_CURRENT_REVOLVING, 0) - COALESCE(ago6.SBFEUTIL_CURRENT_REVOLVING, 0)  AS SBFE_util_change_6m
    FROM account_month_spine s
    LEFT JOIN sbfe_monthly cur  ON cur.account_id  = s.account_id AND cur.bureau_month = s.cohort_month
    LEFT JOIN sbfe_monthly ago3 ON ago3.account_id = s.account_id AND ago3.bureau_month = DATEADD('month', -3, s.cohort_month)
    LEFT JOIN sbfe_monthly ago6 ON ago6.account_id = s.account_id AND ago6.bureau_month = DATEADD('month', -6, s.cohort_month)
),

-- ══════════════════════════════════════════════════════════════
-- FINAL ACCOUNT-MONTH RAW METRICS (no rolling windows)
-- ══════════════════════════════════════════════════════════════
monthly_combined AS (
    SELECT
        s.account_id,
        s.cohort_month,
        -- spend
        COALESCE(mm.gross_spend_mth, 0)         AS gross_spend_mth,
        COALESCE(mm.fuel_spend_mth, 0)          AS fuel_spend_mth,
        COALESCE(mm.INN_spend_mth, 0)           AS INN_spend_mth,
        COALESCE(mm.OON_spend_mth, 0)           AS OON_spend_mth,
        -- gallons
        COALESCE(mm.gallons_mth, 0)             AS gallons_mth,
        COALESCE(mm.INN_gallons_mth, 0)         AS INN_gallons_mth,
        COALESCE(mm.OON_gallons_mth, 0)         AS OON_gallons_mth,
        -- transactions
        COALESCE(mm.transaction_count_mth, 0)   AS transaction_count_mth,
        COALESCE(dt.declined_txn, 0)            AS declined_txn_mth,
        CASE WHEN COALESCE(mm.transaction_count_mth, 0) > 0 THEN 1 ELSE 0 END AS is_active_month,
        -- revenue
        COALESCE(rm.revenue_amount_usd, 0)      AS revenue_mth,
        -- fees
        COALESCE(fm.late_fee_mth, 0)            AS late_fee_mth,
        COALESCE(fm.servfee_mth, 0)             AS servfee_mth,
        COALESCE(fm.delivery_fee_mth, 0)        AS delivery_fee_mth,
        COALESCE(fm.oth_card_fee_mth, 0)        AS oth_card_fee_mth,
        COALESCE(fm.reactive_fee_mth, 0)        AS reactive_fee_mth,
        COALESCE(fm.rebate_mth, 0)              AS rebate_mth,
        COALESCE(fm.edgefuel_mth, 0)            AS edgefuel_mth,
        COALESCE(fm.interest_mth, 0)            AS interest_mth,
        COALESCE(fm.revshare_mth, 0)            AS revshare_mth,
        COALESCE(fm.pmf_mth, 0)                 AS pmf_mth,
        COALESCE(fm.disrev_mth, 0)              AS disrev_mth,
        COALESCE(fm.late_fee_mth,0) + COALESCE(fm.servfee_mth,0) + COALESCE(fm.delivery_fee_mth,0)
            + COALESCE(fm.oth_card_fee_mth,0) + COALESCE(fm.reactive_fee_mth,0) AS total_fee_mth,
        -- SRs
        COALESCE(sr.sr_count_mth, 0)            AS sr_count_mth,
        COALESCE(sr.sr_closed_mth, 0)           AS sr_closed_mth,
        -- Case types
        COALESCE(sct.case_total_count, 0)                   AS case_total_count,
        COALESCE(sct.case_customer_current_count, 0)        AS case_customer_current_count,
        COALESCE(sct.case_null_count, 0)                    AS case_null_count,
        COALESCE(sct.case_account_count, 0)                 AS case_account_count,
        COALESCE(sct.case_payment_billing_count, 0)         AS case_payment_billing_count,
        COALESCE(sct.case_online_ivr_ma_count, 0)           AS case_online_ivr_ma_count,
        COALESCE(sct.case_card_maintenance_count, 0)        AS case_card_maintenance_count,
        COALESCE(sct.case_fraud_count, 0)                   AS case_fraud_count,
        COALESCE(sct.case_card_declined_count, 0)           AS case_card_declined_count,
        COALESCE(sct.case_fee_waiver_count, 0)              AS case_fee_waiver_count,
        COALESCE(sct.case_junk_email_count, 0)              AS case_junk_email_count,
        COALESCE(sct.case_payment_billing_rec_count, 0)     AS case_payment_billing_rec_count,
        COALESCE(sct.case_termination_non_vas_count, 0)     AS case_termination_non_vas_count,
        COALESCE(sct.case_authorization_count, 0)           AS case_authorization_count,
        COALESCE(sct.case_application_fraud_count, 0)       AS case_application_fraud_count,
        COALESCE(sct.case_claims_count, 0)                  AS case_claims_count,
        COALESCE(sct.case_fees_count, 0)                    AS case_fees_count,
        COALESCE(sct.case_disputes_count, 0)                AS case_disputes_count,
        COALESCE(sct.case_other_count, 0)                   AS case_other_count,
        -- credit / balance
        COALESCE(ag.cr_limit_mth, 0)            AS cr_limit_mth,
        COALESCE(ag.OUTSTANDING_BALANCE, 0)     AS outstanding_balance_mth
    FROM account_month_spine s
    LEFT JOIN monthly_metrics mm  ON mm.account_id = s.account_id AND mm.metric_month = s.cohort_month
    LEFT JOIN revenue_monthly rm  ON rm.account_id = s.account_id AND DATE_TRUNC('month', rm.calendar_date) = s.cohort_month
    LEFT JOIN fees_monthly fm     ON fm.account_id = s.account_id AND fm.fee_month = s.cohort_month
    LEFT JOIN sr_monthly sr       ON sr.account_id = s.account_id AND sr.sr_month = s.cohort_month
    LEFT JOIN sr_monthly_casetypes sct ON sct.account_id = s.account_id AND sct.case_month = s.cohort_month
    LEFT JOIN aging_metrics ag    ON ag.CUST_ID = s.account_id AND ag.MTH_YR = s.cohort_month
    LEFT JOIN declined_txn dt     ON dt.account_id = s.account_id AND dt.month_yr = s.cohort_month
)

-- ══════════════════════════════════════════════════════════════
-- FINAL SELECT  — account × month raw snapshot
-- ══════════════════════════════════════════════════════════════
SELECT
    -- ── GRAIN ──
    s.account_id,
    s.cohort_month,

    -- ── STATIC / IDENTITY ──
    s.business_entity,
    s.business_program_name,
    s.partner,
    s.partner_ind,
    s.is_vip_ind,
    s.is_govt_ind,
    s.ACQUISITION_YEAR,
    s.ACQUISITION_MONTH,
    s.account_open_date,
    s.account_closed_date,
    s.account_status,
    DATEDIFF('month', s.account_open_date, s.cohort_month) AS tenure_months,

    -- ── ACCOUNT CHARACTERISTICS ──
    ac.acquisition_fico,
    ac.credit_bureau_score,
    ac.approved_credit_limit,
    ac.app_fleet_size,
    ac.years_in_business,
    ac.risk_grade,
    ac.risk_grade_bucket,
    ac.campaign_type_sf,
    ac.fraud_decision,
    ac.sales_type,
    ac.campaign_tactic,
    ac.is_fraud_ind,

    -- ── NAICS ──
    am.naics_description,
    am.naics_industry_code,
    nm.is_trucking_industry,

    -- ── FLEET (monthly snapshot) ──
    fsm.max_active_cardcnt_mth,
    fsm.outstanding_cardcnt_mth,

    -- ── SMALL BUSINESS FLAG ──
    -- Based on outstanding cards in the current month snapshot
    CASE
        WHEN COALESCE(fsm.outstanding_cardcnt_mth, 0) < 10 THEN 'Yes'
        ELSE 'No'
    END AS is_small_biz,

    -- ── FIRST TRANSACTION ──
    ft.first_transaction_dt,
    DATEDIFF('day', s.account_open_date, ft.first_transaction_dt) AS days_to_first_txn,

    -- ── CURRENT MONTH RAW METRICS ──
    mc.gross_spend_mth,
    mc.fuel_spend_mth,
    mc.INN_spend_mth,
    mc.OON_spend_mth,
    mc.gallons_mth,
    mc.INN_gallons_mth,
    mc.OON_gallons_mth,
    mc.transaction_count_mth,
    mc.declined_txn_mth,
    mc.is_active_month,
    mc.revenue_mth,
    mc.late_fee_mth,
    mc.servfee_mth,
    mc.delivery_fee_mth,
    mc.oth_card_fee_mth,
    mc.reactive_fee_mth,
    mc.rebate_mth,
    mc.edgefuel_mth,
    mc.interest_mth,
    mc.revshare_mth,
    mc.pmf_mth,
    mc.disrev_mth,
    mc.total_fee_mth,
    mc.sr_count_mth,
    mc.sr_closed_mth,
    mc.case_total_count,
    mc.case_customer_current_count,
    mc.case_null_count,
    mc.case_account_count,
    mc.case_payment_billing_count,
    mc.case_online_ivr_ma_count,
    mc.case_card_maintenance_count,
    mc.case_fraud_count,
    mc.case_card_declined_count,
    mc.case_fee_waiver_count,
    mc.case_junk_email_count,
    mc.case_payment_billing_rec_count,
    mc.case_termination_non_vas_count,
    mc.case_authorization_count,
    mc.case_application_fraud_count,
    mc.case_claims_count,
    mc.case_fees_count,
    mc.case_disputes_count,
    mc.case_other_count,
    mc.cr_limit_mth,
    mc.outstanding_balance_mth,

    -- ── AGING / CREDIT ──
    ag.WX_DPD_MTH,
    ag.TOTAL_EXPOSURE_MTH,

    -- ── SBFE BUREAU FEATURES ──
    sb.SBFE_hit_flag,
    sb.SBFEACCOUNT_COUNT,
    sb.SBFEOPEN_LINE_COUNT,
    sb.SBFEOPEN_CARD_COUNT,
    sb.SBFERECENT_BALANCE_AMT,
    sb.SBFECLOSED_COUNT,
    sb.SBFECLOSED_INVOLUNTARY_COUNT,
    sb.SBFEUTIL_CURRENT_REVOLVING,
    sb.SBFE_acct_3m_ago,
    sb.SBFE_opnline_3m_ago,
    sb.SBFE_opncard_3m_ago,
    sb.SBFE_bal_3m_ago,
    sb.SBFE_closdcnt_3m_ago,
    sb.SBFE_closd_involcnt_3m_ago,
    sb.SBFE_util_3m_ago,
    sb.SBFE_acct_6m_ago,
    sb.SBFE_opnline_6m_ago,
    sb.SBFE_opncard_6m_ago,
    sb.SBFE_bal_6m_ago,
    sb.SBFE_closdcnt_6m_ago,
    sb.SBFE_closd_involcnt_6m_ago,
    sb.SBFE_util_6m_ago,
    sb.SBFE_acct_change_3m,
    sb.SBFE_opnline_change_3m,
    sb.SBFE_opncard_change_3m,
    sb.SBFE_bal_change_3m,
    sb.SBFE_closdcnt_change_3m,
    sb.SBFE_closd_involcnt_change_3m,
    sb.SBFE_util_change_3m,
    sb.SBFE_acct_change_6m,
    sb.SBFE_opnline_change_6m,
    sb.SBFE_opncard_change_6m,
    sb.SBFE_bal_change_6m,
    sb.SBFE_closdcnt_change_6m,
    sb.SBFE_closd_involcnt_change_6m,
    sb.SBFE_util_change_6m,

    -- ── FORWARD-LOOKING CLOSURE FLAGS ──
    CASE
        WHEN s.account_closed_date IS NOT NULL
         AND YEAR(s.account_closed_date) < 3000
         AND s.account_closed_date >= s.cohort_month
         AND s.account_closed_date <  DATEADD('month', 3, s.cohort_month)
        THEN 1 ELSE 0
    END AS closed_next_3m_flag,

    CASE
        WHEN s.account_closed_date IS NOT NULL
         AND YEAR(s.account_closed_date) < 3000
         AND s.account_closed_date >= s.cohort_month
         AND s.account_closed_date <  DATEADD('month', 6, s.cohort_month)
        THEN 1 ELSE 0
    END AS closed_next_6m_flag,

    CASE
        WHEN s.account_closed_date IS NOT NULL
         AND YEAR(s.account_closed_date) < 3000
         AND s.account_closed_date >= s.cohort_month
         AND s.account_closed_date <  DATEADD('month', 8, s.cohort_month)
        THEN 1 ELSE 0
    END AS closed_next_8m_flag,

    CASE
        WHEN s.account_closed_date IS NOT NULL
         AND YEAR(s.account_closed_date) < 3000
         AND s.account_closed_date >= s.cohort_month
         AND s.account_closed_date <  DATEADD('month', 12, s.cohort_month)
        THEN 1 ELSE 0
    END AS closed_next_12m_flag

FROM account_month_spine s

LEFT JOIN monthly_combined mc
    ON mc.account_id = s.account_id AND mc.cohort_month = s.cohort_month

LEFT JOIN account_characteristics ac
    ON ac.account_id = s.account_id

LEFT JOIN account_metrics am
    ON am.account_id = s.account_id

LEFT JOIN fleet_snapshot_mthly fsm
    ON fsm.account_id = s.account_id AND fsm.mth_yr = s.cohort_month

LEFT JOIN first_transaction ft
    ON ft.account_id = s.account_id

LEFT JOIN naics_mapping nm
    ON nm.account_id = s.account_id

LEFT JOIN declined_txn dt
    ON dt.account_id = s.account_id AND dt.month_yr = s.cohort_month

LEFT JOIN aging_metrics ag
    ON ag.CUST_ID = s.account_id AND ag.MTH_YR = s.cohort_month

LEFT JOIN sbfe_features sb
    ON sb.account_id = s.account_id AND sb.cohort_month = s.cohort_month

ORDER BY s.account_id, s.cohort_month
;


select * from  WORKSPACE.digitalda_stage.Account_ID_features_monthly limit 1000;
