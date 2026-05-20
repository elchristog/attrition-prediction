CREATE OR REPLACE TABLE WORKSPACE.digitalda_stage.entity_Christian_target_variable as   -- option-A output only
-- CREATE OR REPLACE TABLE WORKSPACE.digitalda_stage.entity_Acct_map_Christian as   -- option-C output only
WITH 
-- ============================================================================
-- BLOCK 0: THE TIME SPINE & POINT-IN-TIME BRIDGE (SCD TYPE 2)
-- ============================================================================
-- 1. Generate 37 months (24 for reporting + 12 warm-up months for lags/history)
CALENDAR AS (
    SELECT LAST_DAY(ADD_MONTHS(DATE_TRUNC('month', CURRENT_DATE()), -SEQ4())) AS OBS_DATE
    FROM TABLE(GENERATOR(ROWCOUNT => 37)) 
),

-- 2. THE HISTORICAL BRIDGE (With Infinite Retroactivity Patch)
-- This logic solves the MDM stabilization issue by freezing relationships prior to March 2026.
ANCHOR_RECORDS AS (
    SELECT 
        ACCOUNT_URI,
        ORGANIZATION_URI AS ORG_URI
    FROM PREP.MDM_RELTIO.f_entity_wxaccountnumber_organization_snapshot
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ACCOUNT_URI 
        ORDER BY 
            -- Priority 1: Was active exactly on March 1, 2026
            CASE WHEN ROW_EFF_BEGIN_DTTM <= '2026-03-01'::DATE AND (ROW_EFF_END_DTTM IS NULL OR ROW_EFF_END_DTTM > '2026-03-01'::DATE) THEN 1 
            -- Priority 2: Started after March 1, 2026 (get the earliest one)
                 WHEN ROW_EFF_BEGIN_DTTM > '2026-03-01'::DATE THEN 2
            -- Priority 3: Ended before March 1, 2026 (get the latest one)
                 ELSE 3 END,
            CASE WHEN ROW_EFF_BEGIN_DTTM > '2026-03-01'::DATE THEN ROW_EFF_BEGIN_DTTM END ASC,
            ROW_EFF_BEGIN_DTTM DESC
    ) = 1
),

HISTORICAL_BRIDGE AS (
    -- Standard SCD2 Condition for reliable periods (On or after March 2026)
    SELECT 
        c.OBS_DATE,
        snap.ACCOUNT_URI,
        snap.ORGANIZATION_URI AS ORG_URI
    FROM CALENDAR c
    JOIN PREP.MDM_RELTIO.f_entity_wxaccountnumber_organization_snapshot snap
        ON c.OBS_DATE >= '2026-03-01'::DATE
        AND c.OBS_DATE >= snap.ROW_EFF_BEGIN_DTTM 
        AND (c.OBS_DATE < snap.ROW_EFF_END_DTTM OR snap.ROW_EFF_END_DTTM IS NULL)
        
    UNION ALL
    
    -- Retroactive patch for dates BEFORE March 2026
    SELECT 
        c.OBS_DATE,
        a.ACCOUNT_URI,
        a.ORG_URI
    FROM CALENDAR c
    CROSS JOIN ANCHOR_RECORDS a
    WHERE c.OBS_DATE < '2026-03-01'::DATE
),

-- ============================================================================
-- BLOCK 1: STATIC ACCOUNT DIMENSION (Data Cleaning)
-- ============================================================================
LATEST_NAA AS (
    SELECT 
        SOURCE_ACCOUNT_ID, 
        ATTRITION_TYPE,
        -- Clean 9999-12-31 dummy dates to avoid breaking time calculations
        CASE 
            WHEN ACCOUNT_CLOSED_DATE >= '3000-01-01'::DATE THEN NULL 
            ELSE ACCOUNT_CLOSED_DATE 
        END AS ACCOUNT_CLOSED_DATE
    FROM FINANCE_ANALYTICS.NAM_PORTFOLIO_METRICS.NAM_ACCOUNT_ATTRITION
    QUALIFY ROW_NUMBER() OVER (PARTITION BY SOURCE_ACCOUNT_ID ORDER BY VOLUME_MONTH DESC) = 1
),

-- ============================================================================
-- BLOCK 2: HISTORICAL RISK (AR Past Performance)
-- ============================================================================
AR_ALL_HISTORICAL AS (
    SELECT CUST_ID, BUSINESS_DATE, WX_DAYS_PAST_DUE, WX_AGE99, WX_EIPP_BALANCE, WX_RCRSE_CODE, CR_LIMIT
    FROM PREP.FIN__SYSADM.PS_WX_CUST_DAILY
    UNION ALL
    SELECT CUST_ID, BUSINESS_DATE, WX_DAYS_PAST_DUE, WX_AGE99, WX_EIPP_BALANCE, WX_RCRSE_CODE, CR_LIMIT
    FROM PREP.FIN__SYSADM_ARCH.PS_WX_CSTDAY_ARCH
    UNION ALL
    SELECT CUST_ID, BUSINESS_DATE, WX_DAYS_PAST_DUE, WX_AGE99, WX_EIPP_BALANCE, WX_RCRSE_CODE, CR_LIMIT
    FROM PREP.FIN__SYSADM_ARCH.PS_WX_CUST_DAILY_ARCH
),
AR_LATEST_DAY_PER_MONTH AS (
    SELECT 
        CUST_ID, 
        DATE_TRUNC('month', BUSINESS_DATE) AS VOL_MONTH,
        WX_DAYS_PAST_DUE, WX_AGE99, WX_EIPP_BALANCE, WX_RCRSE_CODE, CR_LIMIT
    FROM AR_ALL_HISTORICAL
    QUALIFY RANK() OVER (PARTITION BY CUST_ID, DATE_TRUNC('month', BUSINESS_DATE) ORDER BY BUSINESS_DATE DESC) = 1
),
AR_MONTHLY_CONSOLIDATED AS (
    SELECT 
        CUST_ID, 
        VOL_MONTH,
        MAX(CR_LIMIT) AS CR_LIMIT, 
        MAX(WX_DAYS_PAST_DUE) AS WX_DAYS_PAST_DUE,
        SUM(COALESCE(WX_AGE99, 0) + COALESCE(WX_EIPP_BALANCE, 0)) AS TOTAL_EXPOSURE,
        -- Identify Fraud/Bankruptcy codes for exclusion
        MAX(CASE WHEN WX_RCRSE_CODE IN ('82','92', 'LI', 'LB') THEN 1 ELSE 0 END) AS HAS_FRAUD_BANKRUPTCY
    FROM AR_LATEST_DAY_PER_MONTH
    GROUP BY CUST_ID, VOL_MONTH
),

-- ============================================================================
-- BLOCK 3: HISTORICAL MONTHLY JOIN (Aggregating Accounts to Entity per Month)
-- ============================================================================
MONTHLY_ACCT_DATA AS (
    SELECT 
        hb.ORG_URI,
        hb.OBS_DATE,
        wx.ACCOUNTNUMBER,
        ln.ACCOUNT_CLOSED_DATE,
        ln.ATTRITION_TYPE,
        
        COALESCE(naa.OUTSTANDING_CARD_COUNT, 0) AS OUTSTANDING_CARD_COUNT,
        COALESCE(naa.ACTIVE_CARD_COUNT, 0) AS ACTIVE_CARD_COUNT,
        COALESCE(naa.PURCHASE_GALLONS_QTY, 0) AS PURCHASE_GALLONS_QTY,
        COALESCE(naa.WEX_TRANSACTION_COUNT, 0) AS WEX_TRANSACTION_COUNT,
        COALESCE(naa.ACCOUNT_TENURE_MONTHS, 0) AS ACCOUNT_TENURE_MONTHS,
        COALESCE(ar.CR_LIMIT, 0) AS CR_LIMIT,
        
        -- Business Risk Logic: DPD > 60 with exposure or specific Fraud codes
        CASE 
            WHEN ar.HAS_FRAUD_BANKRUPTCY = 1 THEN 1 
            WHEN ar.TOTAL_EXPOSURE > 175 AND ar.WX_DAYS_PAST_DUE >= 60 THEN 1 
            ELSE 0 
        END AS HAS_CREDIT_RISK

    FROM HISTORICAL_BRIDGE hb
    JOIN PREP.MDM_RELTIO.ENTITY_WXACCOUNTNUMBER wx ON hb.ACCOUNT_URI = wx.URI
    LEFT JOIN LATEST_NAA ln ON wx.ACCOUNTNUMBER = ln.SOURCE_ACCOUNT_ID
    
    LEFT JOIN FINANCE_ANALYTICS.NAM_PORTFOLIO_METRICS.NAM_ACCOUNT_ATTRITION naa 
        ON wx.ACCOUNTNUMBER = naa.SOURCE_ACCOUNT_ID AND LAST_DAY(naa.VOLUME_MONTH) = hb.OBS_DATE
        
    LEFT JOIN AR_MONTHLY_CONSOLIDATED ar 
        ON wx.ACCOUNTNUMBER = ar.CUST_ID AND DATE_TRUNC('month', hb.OBS_DATE) = ar.VOL_MONTH

    WHERE (wx.SOURCEACCOUNTTYPE IS NULL OR wx.SOURCEACCOUNTTYPE IN ('Account', 'EFS Contract'))
    AND (
        (LENGTH(wx.ACCOUNTNUMBER) IN (10, 13) AND wx.ACCOUNTNUMBER NOT LIKE '%-%') 
        OR (wx.ACCOUNTNUMBER LIKE '%-%') 
    )
),

ENTITY_MONTHLY_VOL AS (
    SELECT 
        ORG_URI, 
        OBS_DATE,
        SUM(OUTSTANDING_CARD_COUNT) AS ENT_OUTSTANDING_CARDS,
        SUM(ACTIVE_CARD_COUNT) AS ENT_ACTIVE_CARDS,
        SUM(PURCHASE_GALLONS_QTY) AS ENT_GALLONS,
        SUM(WEX_TRANSACTION_COUNT) AS ENT_TXNS,
        MAX(ACCOUNT_TENURE_MONTHS) AS ENT_MAX_ACTIVE_MONTHS,
        SUM(CR_LIMIT) AS ENT_CREDIT_LIMIT,
        
        COUNT(DISTINCT ACCOUNTNUMBER) AS ACCOUNT_COUNT,
        
        COUNT(DISTINCT CASE 
            WHEN ACCOUNT_CLOSED_DATE IS NULL OR ACCOUNT_CLOSED_DATE > OBS_DATE THEN ACCOUNTNUMBER 
        END) AS ACTIVE_COUNT,
        
        MAX(CASE WHEN ACCOUNT_CLOSED_DATE <= OBS_DATE THEN ACCOUNT_CLOSED_DATE END) AS ORG_MAX_CLOSED_DATE_AS_OF_OBS,
        
        MAX(HAS_CREDIT_RISK) AS HAS_CURRENT_CREDIT_RISK,
        MAX(CASE WHEN ACCOUNT_CLOSED_DATE >= DATEADD('day', -120, OBS_DATE) AND ACCOUNT_CLOSED_DATE <= OBS_DATE AND ATTRITION_TYPE = 'Involuntary' THEN 1 ELSE 0 END) AS HAS_INVOLUNTARY_CLOSE_120D,
        MAX(CASE WHEN ACCOUNT_CLOSED_DATE >= DATEADD('day', -120, OBS_DATE) AND ACCOUNT_CLOSED_DATE <= OBS_DATE AND ATTRITION_TYPE = 'Conversion' THEN 1 ELSE 0 END) AS HAS_KNOWN_CONVERSION_120D

    FROM MONTHLY_ACCT_DATA
    GROUP BY 1, 2 
),

-- ============================================================================
-- BLOCK 4: THE TIME MACHINE (Historical Trend Safeguards)
-- ============================================================================
ENTITY_TIME_TRAVEL AS (
    SELECT 
        *,
        -- SAFETY: Count consecutive MDM months to ensure data maturity
        COUNT(OBS_DATE) OVER (
            PARTITION BY ORG_URI 
            ORDER BY OBS_DATE 
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS MDM_HISTORY_MONTHS_AVAILABLE,

        -- SAFETY: Track highest tenure to prevent dormant accounts from appearing as "New"
        MAX(ENT_MAX_ACTIVE_MONTHS) OVER (
            PARTITION BY ORG_URI 
            ORDER BY OBS_DATE 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS REAL_MAX_TENURE,

        -- Historical Volume Lags (L1 to L5)
        COALESCE(LAG(ENT_GALLONS, 1) OVER (PARTITION BY ORG_URI ORDER BY OBS_DATE), 0) AS GAL_L1,
        COALESCE(LAG(ENT_GALLONS, 2) OVER (PARTITION BY ORG_URI ORDER BY OBS_DATE), 0) AS GAL_L2,
        COALESCE(LAG(ENT_GALLONS, 3) OVER (PARTITION BY ORG_URI ORDER BY OBS_DATE), 0) AS GAL_L3,
        COALESCE(LAG(ENT_GALLONS, 4) OVER (PARTITION BY ORG_URI ORDER BY OBS_DATE), 0) AS GAL_L4,
        COALESCE(LAG(ENT_GALLONS, 5) OVER (PARTITION BY ORG_URI ORDER BY OBS_DATE), 0) AS GAL_L5,
        
        LAG(ENT_CREDIT_LIMIT, 1) IGNORE NULLS OVER (PARTITION BY ORG_URI ORDER BY OBS_DATE) AS CL_L1,
        LAG(ENT_CREDIT_LIMIT, 3) IGNORE NULLS OVER (PARTITION BY ORG_URI ORDER BY OBS_DATE) AS CL_L3,
        
        -- Moving Average for drop-off detection
        COALESCE(AVG(ENT_GALLONS) OVER (PARTITION BY ORG_URI ORDER BY OBS_DATE ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 0) AS AVG_GAL_RECENT,
        COALESCE(LAG(ENT_GALLONS, 12) OVER (PARTITION BY ORG_URI ORDER BY OBS_DATE), 0) AS GAL_L12,

        -- LOOK-AHEAD: Maximum volume in the next 4 months to ensure Attrition is a Terminal State
        MAX(ENT_GALLONS) OVER (
            PARTITION BY ORG_URI 
            ORDER BY OBS_DATE 
            ROWS BETWEEN 1 FOLLOWING AND 4 FOLLOWING
        ) AS MAX_GAL_NEXT_4M,
        
        -- Count of future months available (to handle the most recent data correctly)
        COUNT(*) OVER (
            PARTITION BY ORG_URI 
            ORDER BY OBS_DATE 
            ROWS BETWEEN 1 FOLLOWING AND 4 FOLLOWING
        ) AS FUTURE_MONTHS_AVAILABLE
    FROM ENTITY_MONTHLY_VOL
),

SILENT_ATTRITION_EVAL AS (
    SELECT 
        ORG_URI,
        OBS_DATE,
        
        CASE 
            -- WAIVER: We waive the 6-month MDM history requirement for dates prior to March 2026 
            -- to allow the Infinite Retroactivity patch to label the historical training set.
            WHEN MDM_HISTORY_MONTHS_AVAILABLE < 6 AND OBS_DATE >= '2026-03-01'::DATE THEN 0
            
            -- WAIVER: Physical account must have existed for at least 6 months
            WHEN COALESCE(REAL_MAX_TENURE, 0) < 6 THEN 0
            
            -- PERSISTENCE CHECK: If the entity recovered within the next 4 months, it is NOT attrition.
            -- We allow a small threshold (5 gallons) to account for data noise/corrections.
            WHEN FUTURE_MONTHS_AVAILABLE > 0 AND MAX_GAL_NEXT_4M > 5 THEN 0

            -- SILENT ATTRITION RULES (Only apply if Persistence Check passed)
            WHEN ENT_GALLONS = 0 AND GAL_L1 = 0 AND GAL_L2 = 0 AND GAL_L3 = 0 AND GAL_L4 = 0 AND GAL_L5 = 0 THEN 1
            WHEN ENT_ACTIVE_CARDS <= 20 AND ENT_GALLONS = 0 AND GAL_L1 = 0 AND GAL_L2 = 0 AND GAL_L3 > 0  AND ENT_CREDIT_LIMIT <= COALESCE(CL_L3, 0) THEN 1
            WHEN ENT_ACTIVE_CARDS >= 21 AND ENT_GALLONS = 0 AND GAL_L1 = 0 AND GAL_L2 = 0 AND GAL_L3 > 0 AND ENT_CREDIT_LIMIT <= COALESCE(CL_L3, 0) THEN 1
            WHEN AVG_GAL_RECENT > 0 AND GAL_L12 > 0 AND ENT_GALLONS <= (AVG_GAL_RECENT * 0.50) AND ENT_GALLONS <= (GAL_L12 * 0.50) AND ENT_CREDIT_LIMIT <= COALESCE(CL_L1, 0) THEN 1
            ELSE 0 
        END AS IS_SILENT_ATTRITION
    FROM ENTITY_TIME_TRAVEL
),

FINAL_MONTHLY_STATS AS (
    SELECT 
        t.*,
        COALESCE(s.IS_SILENT_ATTRITION, 0) AS IS_SILENT_ATTRITION,
        
        -- Identify "Behavioral Flips": Sudden drop in accounts with closures but not classified as Risk or Silent
        CASE 
            WHEN t.ORG_MAX_CLOSED_DATE_AS_OF_OBS >= DATEADD('day', -120, t.OBS_DATE) 
            AND t.ACTIVE_COUNT > 0
            AND COALESCE(s.IS_SILENT_ATTRITION, 0) = 0
            AND t.HAS_INVOLUNTARY_CLOSE_120D = 0
            THEN 1 ELSE 0
        END AS IS_BEHAVIORAL_FLIP
        
    FROM ENTITY_TIME_TRAVEL t
    JOIN SILENT_ATTRITION_EVAL s ON t.ORG_URI = s.ORG_URI AND t.OBS_DATE = s.OBS_DATE
),

-- ============================================================================
-- BLOCK 5: CLASSIFIED DATA (The Final Labeling Waterfall)
-- ============================================================================
CLASSIFIED_DATA AS (
    SELECT 
        ORG_URI,
        OBS_DATE AS EVALUATION_MONTH,
        ACCOUNT_COUNT,
        ACTIVE_COUNT,
        (ACCOUNT_COUNT - ACTIVE_COUNT) AS INACTIVE_ACCOUNT_COUNT,
        ORG_MAX_CLOSED_DATE_AS_OF_OBS,
        
        CASE 
            -- A. EXCLUSION: Risk and Conversions take absolute priority
            WHEN HAS_KNOWN_CONVERSION_120D = 1 OR IS_BEHAVIORAL_FLIP = 1 THEN 'Event: Flip/Conversion'
            WHEN HAS_CURRENT_CREDIT_RISK = 1 OR HAS_INVOLUNTARY_CLOSE_120D = 1 THEN 'Event: Risk/Involuntary'
            
            -- B. HARD CLOSE: If all accounts are physically closed
            WHEN ACTIVE_COUNT = 0 THEN
                CASE 
                    WHEN ORG_MAX_CLOSED_DATE_AS_OF_OBS >= DATEADD('day', -30, OBS_DATE) THEN 'Event: Voluntary (Hard Close)'
                    ELSE 'Inactive (Graveyard)' 
                END

            -- C. SILENT ATTRITION: Active but zero usage
            WHEN IS_SILENT_ATTRITION = 1 THEN 'State: Voluntary (Silent)' 
            
            -- D. HEALTHY
            WHEN ACTIVE_COUNT > 0 AND IS_SILENT_ATTRITION = 0 THEN 'Healthy' 
            
            -- E. ANOMALY
            ELSE 'Data Anomaly / Ghosts' 
        END AS HISTORICAL_ATTRITION_DECISION

    FROM FINAL_MONTHLY_STATS
)

-- ============================================================================
-- OUTPUT OPTIONS (Toggle as needed)
-- ============================================================================

-- OPTION A: GRANULAR OUTPUT (For ML training set generation)
SELECT 
    ORG_URI,
    EVALUATION_MONTH,
    ACCOUNT_COUNT,
    ACTIVE_COUNT,
    HISTORICAL_ATTRITION_DECISION,
    
    CASE 
        WHEN HISTORICAL_ATTRITION_DECISION = 'Event: Flip/Conversion' THEN ORG_MAX_CLOSED_DATE_AS_OF_OBS 
        ELSE NULL 
    END AS FLIP_DATE,

    CASE 
        WHEN HISTORICAL_ATTRITION_DECISION = 'Event: Risk/Involuntary' THEN ORG_MAX_CLOSED_DATE_AS_OF_OBS
        WHEN HISTORICAL_ATTRITION_DECISION = 'State: Voluntary (Silent)' THEN LAST_DAY(EVALUATION_MONTH)
        WHEN HISTORICAL_ATTRITION_DECISION = 'Event: Voluntary (Hard Close)' THEN ORG_MAX_CLOSED_DATE_AS_OF_OBS
        ELSE NULL 
    END AS ATTRITION_DATE
FROM CLASSIFIED_DATA
WHERE  EVALUATION_MONTH < DATE_TRUNC('month', CURRENT_DATE()) 
  -- AND EVALUATION_MONTH >= DATEADD('month', -24, CURRENT_DATE())
ORDER BY ORG_URI, EVALUATION_MONTH DESC;
