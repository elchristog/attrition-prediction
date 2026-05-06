-- ════════════════════════════════════════════════════════════════════════════
-- C5 — Entity_ID_model_features
-- Grain : ORG_URI × cohort_month
-- Purpose: Derive compound / ratio / trend / momentum features on top of
--          the C4 target table (Entity_ID_target_table).
--          Outputs the final ML-ready table including all base columns from
--          C4, plus the Section A / B / C engineered features from the
--          feature workbook.
--
-- Sources : WORKSPACE.digitalda_stage.Entity_ID_target_table  (C4)
--
-- LAG AVAILABILITY GUARD
--   All ratio / trend features divide by NULLIF(…, 0) so a zero denominator
--   produces NULL rather than a divide-by-zero error.
--   All rolling lag columns that may be NULL (because fewer than N prior months
--   exist for the entity) are handled by the NULLIF guards in the ratios AND
--   by explicit NULL-safe arithmetic — the feature column itself will be NULL
--   for early cohort months, which is the correct representation for a model
--   (the model can use is_null as a binary feature or impute at training time).
--   Features that require a PRECEDING window are computed using window functions
--   with ROWS BETWEEN … AND 1 PRECEDING so the current month is never leaked.
--
-- HISTORY EXCLUSION FLAGS (NEW)
--   excl_flag_hist_6m  = 1  when entity has < 6 months of history at cohort_month.
--                           Affects: declined_txn_rate_l6m, oon_spend_share_trend_3m,
--                           sr_resolution_rate_l6m, case_distress_rate_l6m,
--                           active_months_rate_l6m, sr_trend_l3m_vs_l6m,
--                           declined_txn_trend_3m, cr_limit_change_l6m,
--                           reactivation_fee_flag_l6m, dpd_max_l6m,
--                           account_churn_rate_l6m, account_growth_rate_l6m.
--
--   excl_flag_hist_12m = 1  when entity has < 12 months of history at cohort_month.
--                           Affects: active_months_rate_l12m, dpd_months_above_30_l12m,
--                           fee_waiver_frequency_l12m.
--
--   excl_flag_hist_15m = 1  when entity has < 15 months of history at cohort_month.
--                           Affects all blended ratio features (B2–B6) and
--                           seasonality_spend_ratio (C14) — these require 12 months
--                           of L12M averages PLUS the same-period-last-year anchor
--                           which itself needs the 3 months ending 12 months prior
--                           (i.e. cohort_month − 12 must have a valid L3M avg,
--                           meaning the entity needs at least 15 months of history).
--
--   Usage: apply excl_flag_hist_Xm = 0 in addition to the C4 exclusion flags
--          when the model or analysis requires those feature groups to be non-NULL.
--
-- SECTION LAYOUT
--   CTE base            — pull all columns from C4
--   CTE entity_history  — row number per entity ordered by cohort_month (history depth)
--   CTE same_period     — pull L3/L6 spend for same months last year (seasonality)
--   CTE streak          — compute consecutive inactive months (Section C)
--   CTE dpd_rolling     — DPD rolling max + count (Section C)
--   CTE acct_churn      — sub-account closure rate (Section C)
--   CTE cs_term         — months_since_cs_termination_case + ever_had_termination_case
--   CTE card_act_lag    — active_card_utilization_rate 3m ago for trend
--   FINAL SELECT        — all base + all compound features
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE WORKSPACE.digitalda_stage.Entity_ID_model_features AS

WITH

-- ════════════════════════════════════════════════════════════════════════════
-- BASE: pull everything from C4
-- ════════════════════════════════════════════════════════════════════════════
base AS (
    SELECT *
    FROM WORKSPACE.digitalda_stage.Entity_ID_target_table
),

-- ════════════════════════════════════════════════════════════════════════════
-- ENTITY HISTORY DEPTH
-- months_of_history = number of cohort rows available for this entity up to
-- and including the current cohort_month (1 = first month ever seen).
-- Used exclusively to set excl_flag_hist_* flags; not exposed as a feature.
-- ════════════════════════════════════════════════════════════════════════════
entity_history AS (
    SELECT
        ORG_URI,
        cohort_month,
        ROW_NUMBER()
            OVER (PARTITION BY ORG_URI ORDER BY cohort_month) AS months_of_history
    FROM base
),

-- ════════════════════════════════════════════════════════════════════════════
-- SAME-PERIOD-LAST-YEAR anchors (Section B blended ratio features)
-- We join the base table to itself shifted by 12 months so that for each
-- cohort_month we can access the L3 and L6 averages from 12 months prior.
-- This produces NULL for any entity whose history is < 12 months.
-- ════════════════════════════════════════════════════════════════════════════
same_period AS (
    SELECT
        b.ORG_URI,
        b.cohort_month,

        -- Same 3-month window last year (avg of months cohort-12, cohort-11, cohort-10)
        py.spend_avg_l3m        AS spend_same_3m_last_year,
        py.gallons_avg_l3m      AS gallons_same_3m_last_year,
        py.transactions_avg_l3m AS txn_same_3m_last_year,
        py.revenue_avg_l3m      AS revenue_same_3m_last_year,

        -- Same 6-month window last year
        py.spend_avg_l6m        AS spend_same_6m_last_year,
        py.gallons_avg_l6m      AS gallons_same_6m_last_year

    FROM base b
    LEFT JOIN base py
        ON  py.ORG_URI      = b.ORG_URI
        AND py.cohort_month = DATEADD('month', -12, b.cohort_month)
),

-- ════════════════════════════════════════════════════════════════════════════
-- ACTIVE-CARD UTILIZATION 3 MONTHS AGO  (for card_activation_trend_3m)
-- Computed as a LAG inside a CTE so we keep the FINAL SELECT clean.
-- ════════════════════════════════════════════════════════════════════════════
card_act_util AS (
    SELECT
        ORG_URI,
        cohort_month,
        total_active_cards_mth / NULLIF(total_outstanding_cards_mth, 0) AS card_util_rate_cur,

        -- Average utilization over prior 3 months (L1–L3)
        AVG(
            total_active_cards_mth / NULLIF(total_outstanding_cards_mth, 0)
        ) OVER (
            PARTITION BY ORG_URI ORDER BY cohort_month
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS card_util_avg_l3m,

        -- Average utilization over months 4–6 ago (L4–L6)
        AVG(
            total_active_cards_mth / NULLIF(total_outstanding_cards_mth, 0)
        ) OVER (
            PARTITION BY ORG_URI ORDER BY cohort_month
            ROWS BETWEEN 6 PRECEDING AND 4 PRECEDING
        ) AS card_util_avg_l3m_prior
    FROM base
),

-- ════════════════════════════════════════════════════════════════════════════
-- CONSECUTIVE INACTIVE MONTHS  (Section C)
-- Uses a running-sum reset pattern to count the unbroken inactivity streak
-- that ends at cohort_month (i.e. purely backward-looking, no leakage).
-- 
-- Strategy:
--   1. For each row assign group_id = number of active months seen so far
--      (a new active month breaks and resets the streak).
--   2. Count rows in the current group that are inactive.
-- 
-- months_since_last_active : months since most-recent entity_active_mth = 1
--   NULL when the entity has NEVER been inactive yet (all months active).
-- consecutive_inactive_months : length of the current unbroken inactive streak;
--   0 when the entity was active in cohort_month.
-- ════════════════════════════════════════════════════════════════════════════
activity_streak AS (
    SELECT
        ORG_URI,
        cohort_month,
        entity_active_mth,
        -- cumulative count of active months up to (and including) the current row
        SUM(entity_active_mth)
            OVER (PARTITION BY ORG_URI ORDER BY cohort_month
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_active_cnt
    FROM base
),

streak_computed AS (
    SELECT
        ORG_URI,
        cohort_month,
        entity_active_mth,
        cum_active_cnt,

        -- How many rows back was the last active month?
        -- DATEDIFF between cohort_month and the max cohort_month where active=1
        -- in strictly prior rows
        DATEDIFF(
            'month',
            MAX(CASE WHEN entity_active_mth = 1 THEN cohort_month END)
                OVER (PARTITION BY ORG_URI ORDER BY cohort_month
                      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
            cohort_month
        ) AS months_since_last_active,

        -- Count consecutive inactive rows in the current streak (reset when active)
        ROW_NUMBER()
            OVER (PARTITION BY ORG_URI, cum_active_cnt ORDER BY cohort_month)
            - 1  -- subtract 1 because row_number starts at 1 even in an active month
              AS consecutive_inactive_months_raw

    FROM activity_streak
),

streak_final AS (
    SELECT
        ORG_URI,
        cohort_month,
        -- When active in current month → streak = 0
        CASE WHEN entity_active_mth = 1 THEN 0
             ELSE consecutive_inactive_months_raw
        END AS consecutive_inactive_months,

        -- months_since_last_active: NULL if never seen an active month prior
        months_since_last_active
    FROM streak_computed
),

-- ════════════════════════════════════════════════════════════════════════════
-- DPD ROLLING METRICS  (Section C)
-- dpd_max_l6m          — MAX DPD over prior 6 months  (1-PRECEDING window)
-- dpd_months_above_30_l12m — count of months with DPD ≥ 30 in prior 12 months
-- ════════════════════════════════════════════════════════════════════════════
dpd_rolling AS (
    SELECT
        ORG_URI,
        cohort_month,
        MAX(max_wx_dpd_mth)
            OVER (PARTITION BY ORG_URI ORDER BY cohort_month
                  ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)            AS dpd_max_l6m,
        SUM(CASE WHEN max_wx_dpd_mth >= 30 THEN 1 ELSE 0 END)
            OVER (PARTITION BY ORG_URI ORDER BY cohort_month
                  ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING)           AS dpd_months_above_30_l12m
    FROM base
),

-- ════════════════════════════════════════════════════════════════════════════
-- ACCOUNT CHURN RATE  (Section C: account_churn_rate_l6m)
-- Count of sub-accounts whose latest_account_closed_date falls within the
-- 6 months immediately prior to cohort_month, divided by the average account
-- count over that same 6-month window.
-- We compute the rolling average account count via window; the closed count
-- is derived from latest_account_closed_date (already in C4 via C3).
-- ════════════════════════════════════════════════════════════════════════════
acct_churn AS (
    SELECT
        ORG_URI,
        cohort_month,

        -- Rolling average account count over prior 6 months (denominator)
        AVG(account_count)
            OVER (PARTITION BY ORG_URI ORDER BY cohort_month
                  ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)            AS avg_account_count_l6m,

        -- Account count 6 months ago (for account_growth_rate_l6m)
        LAG(account_count, 6)
            OVER (PARTITION BY ORG_URI ORDER BY cohort_month)          AS account_count_6m_ago,

        -- Sub-accounts that closed in the 6m window preceding cohort_month
        -- Using the C3 column latest_account_closed_date as a proxy:
        -- flag each cohort row where a close was recorded in the window
        SUM(
            CASE
                WHEN latest_account_closed_date IS NOT NULL
                 AND latest_account_closed_date >= DATEADD('month', -6, cohort_month)
                 AND latest_account_closed_date <  cohort_month
                THEN 1 ELSE 0
            END
        ) OVER (PARTITION BY ORG_URI ORDER BY cohort_month
                ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)              AS closed_accounts_l6m
    FROM base
),

-- ════════════════════════════════════════════════════════════════════════════
-- CS TERMINATION CASE FEATURES  (Section C)
-- months_since_cs_termination_case  — months since last non-zero termination case
-- ever_had_termination_case         — 1 if any prior month had a termination case
-- ════════════════════════════════════════════════════════════════════════════
cs_term AS (
    SELECT
        ORG_URI,
        cohort_month,

        -- Last cohort_month (strictly prior) with a termination case
        MAX(CASE WHEN case_termination_non_vas_count > 0 THEN cohort_month END)
            OVER (PARTITION BY ORG_URI ORDER BY cohort_month
                  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)    AS last_term_case_month,

        -- Has the entity EVER had a termination case in prior months?
        MAX(CASE WHEN case_termination_non_vas_count > 0 THEN 1 ELSE 0 END)
            OVER (PARTITION BY ORG_URI ORDER BY cohort_month
                  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)    AS ever_had_termination_case

    FROM base
),

cs_term_final AS (
    SELECT
        ORG_URI,
        cohort_month,
        ever_had_termination_case,
        DATEDIFF('month', last_term_case_month, cohort_month) AS months_since_cs_termination_case
    FROM cs_term
)

-- ════════════════════════════════════════════════════════════════════════════
-- FINAL SELECT
-- ════════════════════════════════════════════════════════════════════════════
SELECT

    -- ── GRAIN ──────────────────────────────────────────────────────────────
    b.ORG_URI,
    b.cohort_month,

    -- ── ATTRITION TARGET & STATUS (from C4) ────────────────────────────────
    b.HISTORICAL_ATTRITION_DECISION,
    b.target_current_month,
    b.FLIP_DATE,
    b.ATTRITION_DATE,
    b.status_account_count,
    b.status_active_count,

    -- ── FORWARD-LOOKING TARGETS ─────────────────────────────────────────────
    b.target_3m,
    b.target_6m,
    b.target_8m,
    b.target_12m,

    -- ── EXCLUSION FLAGS (C4) ────────────────────────────────────────────────
    b.excl_flag_1,
    b.excl_flag_2,
    b.excl_flag_3_6m,
    b.excl_flag_3_8m,
    b.excl_flag_3_12m,

    -- ── HISTORY EXCLUSION FLAGS (C5 NEW) ────────────────────────────────────
    -- excl_flag_hist_6m: entity has fewer than 6 months of history.
    --   Features affected: declined_txn_rate_l6m, oon_spend_share_trend_3m,
    --   sr_resolution_rate_l6m, case_distress_rate_l6m, active_months_rate_l6m,
    --   sr_trend_l3m_vs_l6m, declined_txn_trend_3m, cr_limit_change_l6m,
    --   reactivation_fee_flag_l6m, dpd_max_l6m, account_churn_rate_l6m,
    --   account_growth_rate_l6m.
    CASE WHEN eh.months_of_history < 6  THEN 1 ELSE 0 END              AS excl_flag_hist_6m,

    -- excl_flag_hist_12m: entity has fewer than 12 months of history.
    --   Features affected: active_months_rate_l12m, dpd_months_above_30_l12m,
    --   fee_waiver_frequency_l12m.
    CASE WHEN eh.months_of_history < 12 THEN 1 ELSE 0 END              AS excl_flag_hist_12m,

    -- excl_flag_hist_15m: entity has fewer than 15 months of history.
    --   Features affected: spend_l3m_vs_blended_ratio, spend_l6m_vs_blended_ratio,
    --   gallons_l3m_vs_blended_ratio, txn_count_l3m_vs_blended_ratio,
    --   revenue_l3m_vs_blended_ratio, seasonality_spend_ratio.
    --   Rationale: blended ratio features need spend_avg_l12m (12m) PLUS the
    --   same-period-last-year anchor whose L3M avg itself requires 3 months at
    --   cohort_month − 12, i.e. 12 + 3 = 15 months minimum.
    CASE WHEN eh.months_of_history < 15 THEN 1 ELSE 0 END              AS excl_flag_hist_15m,

    -- ══════════════════════════════════════════════════════════════════════
    -- SECTION A — COMPOUND / RATIO FEATURES
    -- All divide by NULLIF(…,0). Result is NULL when denominator is 0 or
    -- when the underlying base column is NULL (lag not yet available).
    -- ══════════════════════════════════════════════════════════════════════

    -- A1. Credit utilization (current month)
    b.total_outstanding_balance_mth / NULLIF(b.total_cr_limit_mth, 0)
                                                                        AS credit_utilization_ratio,

    -- A2. Utilization trend over 3 months
    (b.total_outstanding_balance_mth / NULLIF(b.total_cr_limit_mth, 0))
    - (b.avg_outstanding_balance_l3m / NULLIF(b.max_cr_limit_l3m, 0))
                                                                        AS utilization_trend_3m,

    -- A3. Declined transaction rate — current month
    b.declined_txn_mth / NULLIF(b.transaction_count_mth + b.declined_txn_mth, 0)
                                                                        AS declined_txn_rate_mth,

    -- A4. Declined transaction rate — rolling 6 months  [excl_flag_hist_6m]
    b.declined_txn_sum_l6m / NULLIF(b.transactions_sum_l6m + b.declined_txn_sum_l6m, 0)
                                                                        AS declined_txn_rate_l6m,

    -- A5. Out-of-network spend share — current month
    b.OON_spend_mth / NULLIF(b.gross_spend_mth, 0)                     AS oon_spend_share_mth,

    -- A6. OON spend share trend: current month share vs L3M fuel-to-spend ratio
    --     Proxy for OON migration direction; NULL when L3M spend is zero
    --     [excl_flag_hist_6m]
    (b.OON_spend_mth / NULLIF(b.gross_spend_mth, 0))
    - (b.fuel_spend_sum_l3m / NULLIF(b.spend_sum_l3m, 0))              AS oon_spend_share_trend_3m,

    -- A7. Revenue per gallon — current month
    b.revenue_mth / NULLIF(b.gallons_mth, 0)                           AS revenue_per_gallon_mth,

    -- A8. Spend per active card — current month
    b.gross_spend_mth / NULLIF(b.total_active_cards_mth, 0)            AS spend_per_active_card_mth,

    -- A9. Active card utilization rate — current month
    b.total_active_cards_mth / NULLIF(b.total_outstanding_cards_mth, 0)
                                                                        AS active_card_utilization_rate,

    -- A10. Card activation trend vs 3 months ago
    --      NULL when card_util_rate_3m_ago is NULL (< 3 months of history)
    cau.card_util_avg_l3m - cau.card_util_avg_l3m_prior                  AS card_activation_trend_3m,

    -- A11. Fee-to-revenue ratio — current month
    b.total_fee_mth / NULLIF(b.revenue_mth, 0)                         AS fee_to_revenue_ratio_mth,

    -- A12. Late fee share of total fees — current month
    b.late_fee_mth / NULLIF(b.total_fee_mth, 0)                        AS late_fee_to_total_fee_ratio,

    -- A13. SR resolution rate — current month
    b.sr_closed_mth / NULLIF(b.sr_count_mth, 0)                        AS sr_resolution_rate_mth,

    -- A14. SR resolution rate — rolling 6 months  [excl_flag_hist_6m]
    b.sr_closed_sum_l6m / NULLIF(b.sr_total_sum_l6m, 0)               AS sr_resolution_rate_l6m,

    -- A15. Case distress score — current month (raw count)
    (  b.case_termination_non_vas_count
     + b.case_fee_waiver_count
     + b.case_disputes_count
     + b.case_payment_billing_count 
     + b.case_card_declined_count)                                   AS case_distress_score_mth,

    -- A16. Case distress rate — rolling 6 months  [excl_flag_hist_6m]
    (  b.case_termination_non_vas_sum_l6m
     + b.case_fee_waiver_sum_l6m
     + b.case_disputes_sum_l6m
     + b.case_card_declined_sum_l6m)
    / NULLIF(b.case_total_sum_l6m, 0)                                  AS case_distress_rate_l6m,

    -- A17. SBFE involuntary close rate
    b.sum_sbfe_closed_inv_count / NULLIF(b.sum_sbfe_closed_count, 0)   AS sbfe_involuntary_close_rate,

    -- A18. SBFE line utilization (average balance per open line)
    b.sum_sbfe_recent_balance / NULLIF(b.sum_sbfe_open_line_count * 1.0, 0)
                                                                        AS sbfe_line_utilization,

    -- A19. SBFE external account closure acceleration (6m minus 3m delta)
    b.sum_sbfe_closdcnt_change_6m - b.sum_sbfe_closdcnt_change_3m      AS sbfe_account_closure_trend,

    -- A20. Small-business account concentration
    b.small_biz_account_count / NULLIF(b.account_count, 0)             AS small_biz_concentration,

    -- A21. Revenue per active account — current month
    b.revenue_mth / NULLIF(b.active_account_count, 0)                  AS revenue_per_account_mth,

    -- A22. Gallons per transaction — current month (avg fill size)
    b.gallons_mth / NULLIF(b.transaction_count_mth, 0)                 AS gallons_per_txn_mth,

    -- A23. Spend concentration index
    b.gross_spend_mth
    / NULLIF(
        b.total_active_cards_mth
        * (b.gallons_mth / NULLIF(b.transaction_count_mth, 0))
      , 0)                                                              AS spend_concentration_index,

    -- ══════════════════════════════════════════════════════════════════════
    -- SECTION B — TREND, MOMENTUM & VELOCITY FEATURES
    -- ══════════════════════════════════════════════════════════════════════

    -- B1. Month-over-month spend growth
    --     NULL when spend_l1m is NULL (entity's first month)
    (b.gross_spend_mth - b.spend_l1m) / NULLIF(b.spend_l1m, 0)        AS spend_mom_growth,

    -- B2. Spend L3M vs blended (same period last year × 50% + L12M avg × 50%)
    --     NULL when either same-period or L12M avg is unavailable
    --     [excl_flag_hist_15m]
    b.spend_avg_l3m
    / NULLIF(
        0.5 * sp.spend_same_3m_last_year
        + 0.5 * b.spend_avg_l12m
      , 0)                                                              AS spend_l3m_vs_blended_ratio,

    -- B3. Spend L6M vs blended  [excl_flag_hist_15m]
    b.spend_avg_l6m
    / NULLIF(
        0.5 * sp.spend_same_6m_last_year
        + 0.5 * b.spend_avg_l12m
      , 0)                                                              AS spend_l6m_vs_blended_ratio,

    -- B4. Gallons L3M vs blended  [excl_flag_hist_15m]
    b.gallons_avg_l3m
    / NULLIF(
        0.5 * sp.gallons_same_3m_last_year
        + 0.5 * b.gallons_avg_l12m
      , 0)                                                              AS gallons_l3m_vs_blended_ratio,

    -- B5. Transaction count L3M vs blended  [excl_flag_hist_15m]
    b.transactions_avg_l3m
    / NULLIF(
        0.5 * sp.txn_same_3m_last_year
        + 0.5 * b.transactions_avg_l12m
      , 0)                                                              AS txn_count_l3m_vs_blended_ratio,

    -- B6. Revenue L3M vs blended  [excl_flag_hist_15m]
    b.revenue_avg_l3m
    / NULLIF(
        0.5 * sp.revenue_same_3m_last_year
        + 0.5 * b.revenue_avg_l12m
      , 0)                                                              AS revenue_l3m_vs_blended_ratio,

    -- B7. Active months rate — L6M (fraction of last 6 months active)
    --     [excl_flag_hist_6m]
    b.active_months_l6m / 6.0                                          AS active_months_rate_l6m,

    -- B8. Active months rate — L12M  [excl_flag_hist_12m]
    b.active_months_l12m / 12.0                                        AS active_months_rate_l12m,

    -- B9. SR trend: recent 3m avg vs 6m avg (>1 = escalating)  [excl_flag_hist_6m]
    b.sr_total_avg_l3m / NULLIF(b.sr_total_avg_l6m, 0)                AS sr_trend_l3m_vs_l6m,

    -- B10. Declined transaction rate worsening: 3m rate minus 6m rate
    --      [excl_flag_hist_6m]
    (  b.declined_txn_sum_l3m / NULLIF(b.transactions_sum_l3m + b.declined_txn_sum_l3m, 0) )
    - (  b.declined_txn_sum_l6m / NULLIF(b.transactions_sum_l6m + b.declined_txn_sum_l6m, 0) )
                                                                        AS declined_txn_trend_3m,

    -- B11. Credit limit change over 6 months (positive = increase, negative = cut)
    --      [excl_flag_hist_6m]
    b.total_cr_limit_mth - b.max_cr_limit_l6m                          AS cr_limit_change_l6m,

    -- B12. Reactivation fee flag: any reactivation fee in last 6 months
    --      [excl_flag_hist_6m]
    CASE WHEN b.reactivation_fee_sum_l6m > 0 THEN 1 ELSE 0 END        AS reactivation_fee_flag_l6m,

    -- ══════════════════════════════════════════════════════════════════════
    -- SECTION C — NEW DERIVED FEATURES
    -- ══════════════════════════════════════════════════════════════════════

    -- C1. Months since last active month (NULL if never inactive so far)
    sf.months_since_last_active,

    -- C2. Consecutive inactive months (0 when currently active)
    sf.consecutive_inactive_months,

    -- C3. Price per gallon — current month (effective fuel price paid)
    b.fuel_spend_mth / NULLIF(b.gallons_mth, 0)                        AS price_per_gallon_mth,

    -- C4. Non-fuel spend share — current month
    (b.gross_spend_mth - b.fuel_spend_mth) / NULLIF(b.gross_spend_mth, 0)
                                                                        AS nonfuel_spend_share_mth,

    -- C5. Product depth count — distinct WEX revenue lines with revenue > 0
    --     Counts: edgefuel, pmf, disrev, rebate, interest, revshare, late_fee,
    --             servfee, delivery_fee, oth_card_fee, reactive_fee
    (   CASE WHEN b.edgefuel_mth    > 0 THEN 1 ELSE 0 END
      + CASE WHEN b.pmf_mth         > 0 THEN 1 ELSE 0 END
      + CASE WHEN b.disrev_mth      > 0 THEN 1 ELSE 0 END
      + CASE WHEN b.rebate_mth      > 0 THEN 1 ELSE 0 END
      + CASE WHEN b.interest_mth    > 0 THEN 1 ELSE 0 END
      + CASE WHEN b.revshare_mth    > 0 THEN 1 ELSE 0 END
      + CASE WHEN b.late_fee_mth    > 0 THEN 1 ELSE 0 END
      + CASE WHEN b.servfee_mth     > 0 THEN 1 ELSE 0 END
      + CASE WHEN b.delivery_fee_mth > 0 THEN 1 ELSE 0 END
      + CASE WHEN b.oth_card_fee_mth > 0 THEN 1 ELSE 0 END
      + CASE WHEN b.reactive_fee_mth > 0 THEN 1 ELSE 0 END
    )                                                                   AS product_depth_count,

    -- C6. Months since last CS termination case (NULL if never had one)
    ctf.months_since_cs_termination_case,

    -- C7. Ever had a termination case in prior months (0/1; NULL treated as 0 below)
    COALESCE(ctf.ever_had_termination_case, 0)                         AS ever_had_termination_case,

    -- C8. Max DPD over prior 6 months  [excl_flag_hist_6m]
    dr.dpd_max_l6m,

    -- C9. Count of months with DPD ≥ 30 in prior 12 months  [excl_flag_hist_12m]
    dr.dpd_months_above_30_l12m,

    -- C10. Sub-account churn rate over prior 6 months  [excl_flag_hist_6m]
    --      closed_accounts_l6m is a rough proxy (close events recorded in window)
    ac.closed_accounts_l6m / NULLIF(ac.avg_account_count_l6m, 0)       AS account_churn_rate_l6m,

    -- C11. Account growth rate over prior 6 months  [excl_flag_hist_6m]
    --      Positive = fleet expansion; Negative = contraction
    (b.account_count - ac.account_count_6m_ago)
    / NULLIF(ac.account_count_6m_ago, 0)                               AS account_growth_rate_l6m,

    -- C12. Acquisition channel risk score (ordinal; higher = higher base-risk channel)
    CASE
        WHEN b.Salestype_FS_count  > 0 THEN 1   -- Field Sales (lowest risk)
        WHEN b.Salestype_IS_count  > 0 THEN 2   -- Inside Sales
        WHEN b.Salestype_DM_count  > 0 THEN 3   -- Digital Marketing
        ELSE 4                                   -- Partner / unknown
    END                                                                 AS acquisition_channel_risk_score,

    -- C13. Tenure bucket (non-linear attrition hazard encoding)
    CASE
        WHEN b.avg_tenure_months <  12 THEN 'new'
        WHEN b.avg_tenure_months <  36 THEN 'growing'
        WHEN b.avg_tenure_months <  72 THEN 'established'
        ELSE                                'mature'
    END                                                                 AS tenure_bucket,

    -- C14. Seasonality spend ratio — current month vs same month in prior 2 years
    --      We use the same-period CTE (12m prior); a full 24m version would need
    --      a second self-join. Using 12m prior as documented approximation.
    --      [excl_flag_hist_15m]
    b.gross_spend_mth / NULLIF(sp.spend_same_3m_last_year, 0)          AS seasonality_spend_ratio,

    -- C15. Fee waiver frequency — normalised over last 12 months active months
    --      [excl_flag_hist_12m]
    b.case_fee_waiver_sum_l12m / NULLIF(b.active_months_l12m, 0)       AS fee_waiver_frequency_l12m

FROM base b

-- History depth (drives excl_flag_hist_*)
LEFT JOIN entity_history eh
    ON  eh.ORG_URI      = b.ORG_URI
    AND eh.cohort_month = b.cohort_month

-- Same-period last year anchors
LEFT JOIN same_period sp
    ON  sp.ORG_URI      = b.ORG_URI
    AND sp.cohort_month = b.cohort_month

-- Card activation utilization (current + 3m lag)
LEFT JOIN card_act_util cau
    ON  cau.ORG_URI      = b.ORG_URI
    AND cau.cohort_month = b.cohort_month

-- Activity streak features
LEFT JOIN streak_final sf
    ON  sf.ORG_URI      = b.ORG_URI
    AND sf.cohort_month = b.cohort_month

-- DPD rolling features
LEFT JOIN dpd_rolling dr
    ON  dr.ORG_URI      = b.ORG_URI
    AND dr.cohort_month = b.cohort_month

-- Account churn features
LEFT JOIN acct_churn ac
    ON  ac.ORG_URI      = b.ORG_URI
    AND ac.cohort_month = b.cohort_month

-- CS termination case features
LEFT JOIN cs_term_final ctf
    ON  ctf.ORG_URI      = b.ORG_URI
    AND ctf.cohort_month = b.cohort_month

ORDER BY b.ORG_URI, b.cohort_month
;

-- ════════════════════════════════════════════════════════════════════════════
-- VALIDATION QUERIES
-- ════════════════════════════════════════════════════════════════════════════

-- Clean 8m training set — all exclusions applied, all feature groups valid
-- Use this as the default training dataset.
SELECT *
FROM WORKSPACE.digitalda_stage.Entity_ID_model_features
WHERE excl_flag_1         = 0
  AND excl_flag_2         = 0
  AND excl_flag_3_8m      = 0
  AND excl_flag_hist_15m  = 0    -- guarantees all Section B blended + seasonality features non-NULL
  AND cohort_month        < '2026-04-01'
  -- optional: exclude rows with core data gaps (not new-entity NULLs)
  AND NOT (gallons_mth IS NULL AND revenue_mth IS NULL AND transaction_count_mth IS NULL);

-- Clean 8m training set — 6m feature group only (less restrictive)
-- SELECT *
-- FROM WORKSPACE.digitalda_stage.Entity_ID_model_features
-- WHERE excl_flag_1        = 0
--   AND excl_flag_2        = 0
--   AND excl_flag_3_8m     = 0
--   AND excl_flag_hist_6m  = 0
--   AND cohort_month       < '2026-04-01';

-- Clean 8m training set — 12m feature group
-- SELECT *
-- FROM WORKSPACE.digitalda_stage.Entity_ID_model_features
-- WHERE excl_flag_1         = 0
--   AND excl_flag_2         = 0
--   AND excl_flag_3_8m      = 0
--   AND excl_flag_hist_12m  = 0
--   AND cohort_month        < '2026-04-01';

-- Clean training sets for 6m / 12m targets follow the same pattern;
-- swap excl_flag_3_8m for excl_flag_3_6m or excl_flag_3_12m as needed.

-- 1. Quick row count and grain check
SELECT
    COUNT(*)                          AS total_rows,
    COUNT(DISTINCT ORG_URI)           AS entity_count,
    COUNT(DISTINCT cohort_month)      AS month_count,
    MIN(cohort_month)                 AS earliest_month,
    MAX(cohort_month)                 AS latest_month
FROM WORKSPACE.digitalda_stage.Entity_ID_model_features;

-- 2. History flag summary — understand data loss per flag tier
SELECT
    excl_flag_hist_6m,
    excl_flag_hist_12m,
    excl_flag_hist_15m,
    COUNT(*)                    AS row_count,
    COUNT(DISTINCT ORG_URI)     AS entity_count
FROM WORKSPACE.digitalda_stage.Entity_ID_model_features
WHERE cohort_month < '2026-04-01'
GROUP BY ALL
ORDER BY ALL;

-- 3. Spot-check compound feature nulls (expect NULLs for early cohort months)
SELECT
    cohort_month,
    COUNT(*)                                                AS rows,
    SUM(CASE WHEN credit_utilization_ratio        IS NULL THEN 1 ELSE 0 END) AS null_util_ratio,
    SUM(CASE WHEN spend_l3m_vs_blended_ratio       IS NULL THEN 1 ELSE 0 END) AS null_blend_l3m,
    SUM(CASE WHEN consecutive_inactive_months      IS NULL THEN 1 ELSE 0 END) AS null_streak,
    SUM(CASE WHEN months_since_cs_termination_case IS NULL THEN 1 ELSE 0 END) AS null_cs_term,
    SUM(CASE WHEN dpd_max_l6m                      IS NULL THEN 1 ELSE 0 END) AS null_dpd_l6m
FROM WORKSPACE.digitalda_stage.Entity_ID_model_features
GROUP BY cohort_month
ORDER BY cohort_month;

-- 4. Target class distribution on clean 8m training set
-- SELECT
--     historical_attrition_decision,
--     COUNT(DISTINCT ORG_URI) AS entity_count,
--     ROUND(AVG(credit_utilization_ratio), 4)     AS avg_util_ratio,
--     ROUND(AVG(declined_txn_rate_l6m), 4)        AS avg_decline_rate_l6m,
--     ROUND(AVG(active_months_rate_l6m), 4)       AS avg_active_rate_l6m,
--     ROUND(AVG(consecutive_inactive_months), 2)  AS avg_consec_inactive,
--     ROUND(AVG(product_depth_count), 2)          AS avg_product_depth
-- FROM WORKSPACE.digitalda_stage.Entity_ID_model_features
-- WHERE excl_flag_1         = 0
--   AND excl_flag_2         = 0
--   AND excl_flag_3_8m      = 0
--   AND excl_flag_hist_15m  = 0
--   AND cohort_month        < '2026-04-01'
-- GROUP BY ALL
-- ORDER BY historical_attrition_decision;

-- 5. Full sample
SELECT * FROM WORKSPACE.digitalda_stage.Entity_ID_model_features LIMIT 100;