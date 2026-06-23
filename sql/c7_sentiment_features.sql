-- ====================================================================
-- C7 — Entity_Sentiment_Features_Rolling
-- Grain : org_uri × cohort_month
-- Purpose: Aggregates call-level sentiment metrics to the organization level
-- ====================================================================

CREATE OR REPLACE TABLE WORKSPACE.digitalda_stage.Entity_Sentiment_Features_C7 AS

WITH unified_calls AS (
    -- Era 1: Pre-November 2025
    SELECT 
        DATE_TRUNC('MONTH', v.CALLSTARTDATETIME) AS call_month,
        TRY_TO_NUMBER(v.CUSTOMER_SENTIMENT__C) AS customer_sentiment_score,
        c.ACCOUNT_WEX_ACCOUNT AS nam_account_number
    FROM PREP.SALESFORCE_OWNER.VOICECALL v
    INNER JOIN COMMON.CALLCENTER.SALESFORCE_CASE_DETAILS c 
        ON v.CASE__C = c.CASE_ID
    WHERE v.CUSTOMER_SENTIMENT__C IS NOT NULL 
      AND c.ACCOUNT_WEX_ACCOUNT LIKE '91%'   
      AND LENGTH(c.ACCOUNT_WEX_ACCOUNT) = 13
      AND v.CALLDURATIONINSECONDS >= 60
      AND v.CALLSTARTDATETIME < '2025-11-01'

    UNION ALL

    -- Era 2: November 2025 – Present
    SELECT DISTINCT 
        DATE_TRUNC('MONTH', v.VOICECALL_CREATED_DATE_UTC) AS call_month,
        TRY_TO_NUMBER(v.VOICECALL_CUSTOMER_SENTIMENT_SCORE) AS customer_sentiment_score,
        c.ACCOUNT_WEX_ACCOUNT AS nam_account_number
    FROM COMMON.CALLCENTER.SALESFORCE_VOICECALL v
    INNER JOIN COMMON.CALLCENTER.SALESFORCE_CASE_DETAILS c 
        ON v.VOICECALL_ACCOUNT_ID = c.ACCOUNT_ID 
    WHERE v.VOICECALL_CUSTOMER_SENTIMENT_SCORE IS NOT NULL
      AND v.VOICECALL_QUEUE_NAME LIKE '%NAF%'
      AND v.VOICECALL_CREATED_DATE_UTC >= '2025-11-01'
      AND DATEDIFF(second, v.VOICECALL_START_TIME_UTC, v.VOICECALL_END_TIME_UTC) >= 60
      AND c.ACCOUNT_WEX_ACCOUNT LIKE '91%'   
      AND LENGTH(c.ACCOUNT_WEX_ACCOUNT) = 13
),

-- Map accounts to org_uri using the historical bridge logic
mapped_calls AS (
    SELECT
        snap.organization_uri AS org_uri,
        uc.call_month AS cohort_month,
        uc.customer_sentiment_score
    FROM unified_calls uc
    JOIN PREP.MDM_RELTIO.entity_wxaccountnumber wx ON uc.nam_account_number = wx.accountnumber
    JOIN PREP.MDM_RELTIO.f_entity_wxaccountnumber_organization_snapshot snap ON wx.uri = snap.account_uri
    WHERE uc.call_month >= snap.row_eff_begin_dttm
      AND (uc.call_month < snap.row_eff_end_dttm OR snap.row_eff_end_dttm IS NULL)
),

-- Monthly aggregations per org_uri
monthly_org_sentiment AS (
    SELECT
        org_uri,
        cohort_month,
        COUNT(*) AS calls_count_mth,
        COUNT(CASE WHEN customer_sentiment_score <= -10 THEN 1 END) AS poor_sentiment_count_mth,
        SUM(customer_sentiment_score) AS sentiment_sum_mth,
        COUNT(customer_sentiment_score) AS sentiment_score_count_mth
    FROM mapped_calls
    GROUP BY 1, 2
),

-- Spine of active organizations & cohort months to compute rolling windows cleanly
spine_rolling AS (
    SELECT 
        s.org_uri,
        s.cohort_month,
        COALESCE(m.calls_count_mth, 0) AS calls_count_mth,
        COALESCE(m.poor_sentiment_count_mth, 0) AS poor_sentiment_count_mth,
        COALESCE(m.sentiment_sum_mth, 0) AS sentiment_sum_mth,
        COALESCE(m.sentiment_score_count_mth, 0) AS sentiment_score_count_mth
    FROM WORKSPACE.digitalda_stage.Enterprise_Entity_Features_Rolling s
    LEFT JOIN monthly_org_sentiment m 
        ON s.org_uri = m.org_uri AND s.cohort_month = m.cohort_month
)

SELECT
    org_uri,
    cohort_month,
    
    -- 30-day features
    calls_count_mth AS COUNT_TOTAL_CALLS_30D,
    poor_sentiment_count_mth AS COUNT_POOR_SENTIMENT_CALLS_30D,
    
    -- 90-day rolling weighted average (default to 20.00 if no calls in window)
    COALESCE(
        ROUND(
            SUM(sentiment_sum_mth) OVER (PARTITION BY org_uri ORDER BY cohort_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) /
            NULLIF(SUM(sentiment_score_count_mth) OVER (PARTITION BY org_uri ORDER BY cohort_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 0),
            2
        ),
        20.00
    ) AS AVG_SENTIMENT_SCORE_90D,
    
    -- Sentiment Delta Variance (30d average sentiment minus 90d average sentiment)
    ROUND(
        COALESCE(sentiment_sum_mth / NULLIF(sentiment_score_count_mth, 0), 20.00) -
        COALESCE(
            SUM(sentiment_sum_mth) OVER (PARTITION BY org_uri ORDER BY cohort_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) /
            NULLIF(SUM(sentiment_score_count_mth) OVER (PARTITION BY org_uri ORDER BY cohort_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 0),
            20.00
        ),
        2
    ) AS SENTIMENT_DELTA_VARIANCE,
    
    -- Activity flag
    CASE WHEN calls_count_mth > 0 THEN 1 ELSE 0 END AS HAS_CONTACTED_30D
FROM spine_rolling;
