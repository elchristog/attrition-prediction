-- ====================================================================
-- ATTRITION RECONCILIATION AUDIT QUERIES
-- Purpose: Reconcile and audit the differences between:
--          1. Alaina & Rachel's Account-Level Attrition (Approach 1)
--          2. Christian & Ryan's Entity-Level Attrition (Master Table C4)
-- Date: 2026-06-12
-- ====================================================================

-- ════════════════════════════════════════════════════════════════════
-- QUERY 1: CLOSED ACCOUNTS BY ENTITY ATTRITION DECISION
-- Purpose: Of the accounts Alaina flagged as closed in April 2026,
--          which entity-level attrition decision did their parent organization fall into?
--          This reveals how much of Alaina's churn is "Healthy" (partial account closes)
--          or "Risk/Involuntary" (credit/charge-off exclusions).
-- ════════════════════════════════════════════════════════════════════

WITH 
RELTIO_BRIDGE AS (
    SELECT DISTINCT
        wx.accountnumber AS ACCOUNT_ID,
        hb.org_uri AS ORG_URI,
        hb.cohort_month AS COHORT_MONTH
    FROM WORKSPACE.digitalda_stage.Account_ID_features_monthly af
    JOIN PREP.MDM_RELTIO.entity_wxaccountnumber wx ON af.account_id = wx.accountnumber
    JOIN (
        WITH anchor_records AS (
            SELECT 
                account_uri,
                organization_uri AS org_uri
            FROM PREP.MDM_RELTIO.f_entity_wxaccountnumber_organization_snapshot
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY account_uri 
                ORDER BY
                    CASE WHEN row_eff_begin_dttm <= '2026-03-01'::DATE AND (row_eff_end_dttm IS NULL OR row_eff_end_dttm > '2026-03-01'::DATE) THEN 1
                    WHEN row_eff_begin_dttm > '2026-03-01'::DATE THEN 2
                    ELSE 3 END,
                CASE WHEN row_eff_begin_dttm > '2026-03-01'::DATE THEN row_eff_begin_dttm END ASC,
                row_eff_end_dttm DESC
            ) = 1
        ),
        historical_bridge AS (
            SELECT
                c.cohort_month,
                snap.account_uri,
                snap.organization_uri AS org_uri
            FROM (SELECT DISTINCT cohort_month FROM WORKSPACE.digitalda_stage.Account_ID_features_monthly) c
            JOIN PREP.MDM_RELTIO.f_entity_wxaccountnumber_organization_snapshot snap
                ON c.cohort_month >= '2026-03-01'::DATE
                AND c.cohort_month >= snap.row_eff_begin_dttm
                AND (c.cohort_month < snap.row_eff_end_dttm OR snap.row_eff_end_dttm IS NULL)
            UNION ALL
            SELECT 
                c.cohort_month,
                a.account_uri,
                a.org_uri
            FROM (SELECT DISTINCT cohort_month FROM WORKSPACE.digitalda_stage.Account_ID_features_monthly) c
            CROSS JOIN anchor_records a
            WHERE c.cohort_month < '2026-03-01'::DATE
        )
        SELECT * FROM historical_bridge
    ) hb ON wx.uri = hb.account_uri AND af.cohort_month = hb.cohort_month
),

ACCOUNT_CLOSED AS (
    SELECT 
        SOURCE_ACCOUNT_ID AS ACCOUNT_ID,
        MAX(CASE 
            WHEN ACCOUNT_CLOSED_DATE >= '3000-01-01'::DATE THEN NULL 
            ELSE ACCOUNT_CLOSED_DATE 
        END) AS CLOSED_DATE,
        MAX(ATTRITION_TYPE) AS ATTRITION_TYPE
    FROM FINANCE_ANALYTICS.NAM_PORTFOLIO_METRICS.NAM_ACCOUNT_ATTRITION
    GROUP BY SOURCE_ACCOUNT_ID
),

active_in_prev AS (
    SELECT 
        af.account_id AS ACCOUNT_ID,
        af.gallons_mth AS GALLONS_APR25,
        af.revenue_mth AS REVENUE_APR25,
        COALESCE(b.ORG_URI, 'UNMAPPED') AS ORG_URI,
        ac.CLOSED_DATE,
        ac.ATTRITION_TYPE
    FROM WORKSPACE.digitalda_stage.Account_ID_features_monthly af
    LEFT JOIN RELTIO_BRIDGE b ON af.account_id = b.ACCOUNT_ID AND af.cohort_month = b.COHORT_MONTH
    LEFT JOIN ACCOUNT_CLOSED ac ON af.account_id = ac.ACCOUNT_ID
    WHERE af.cohort_month = '2025-04-01'::DATE
      AND af.partner_ind = 'Wex'
),

alaina_closed_accounts AS (
    SELECT * FROM active_in_prev
    WHERE CLOSED_DATE > '2025-04-01'::DATE
      AND CLOSED_DATE <= '2026-04-30'::DATE
)

SELECT 
    COALESCE(m.HISTORICAL_ATTRITION_DECISION, 'Unmapped Entity / Graveyard') AS ENTITY_ATTRITION_DECISION,
    COUNT(DISTINCT ac.ACCOUNT_ID) AS ACCOUNTS_CLOSED_COUNT,
    SUM(ac.GALLONS_APR25) AS GALLONS_APR25_LOST,
    SUM(ac.REVENUE_APR25) AS REVENUE_APR25_LOST
FROM alaina_closed_accounts ac
LEFT JOIN WORKSPACE.digitalda_stage.ML_Attrition_Master_Table m
    ON ac.ORG_URI = m.ORG_URI
    AND m.cohort_month = '2026-04-01'::DATE
GROUP BY 1
ORDER BY 2 DESC;


-- ════════════════════════════════════════════════════════════════════
-- QUERY 2: SILENT ATTRITION MISSED BY ALAINA
-- Purpose: Identify entities labeled as "Voluntary (Silent)" in April 2026.
--          These entities have no closed dates (accounts remain active on paper)
--          but card activity/volume has completely died to zero.
--          Alaina's report missed these entirely, but our model captures them.
-- ════════════════════════════════════════════════════════════════════

WITH 
RELTIO_BRIDGE AS (
    SELECT DISTINCT
        wx.accountnumber AS ACCOUNT_ID,
        hb.org_uri AS ORG_URI,
        hb.cohort_month AS COHORT_MONTH
    FROM WORKSPACE.digitalda_stage.Account_ID_features_monthly af
    JOIN PREP.MDM_RELTIO.entity_wxaccountnumber wx ON af.account_id = wx.accountnumber
    JOIN (
        WITH anchor_records AS (
            SELECT 
                account_uri,
                organization_uri AS org_uri
            FROM PREP.MDM_RELTIO.f_entity_wxaccountnumber_organization_snapshot
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY account_uri 
                ORDER BY
                    CASE WHEN row_eff_begin_dttm <= '2026-03-01'::DATE AND (row_eff_end_dttm IS NULL OR row_eff_end_dttm > '2026-03-01'::DATE) THEN 1
                    WHEN row_eff_begin_dttm > '2026-03-01'::DATE THEN 2
                    ELSE 3 END,
                CASE WHEN row_eff_begin_dttm > '2026-03-01'::DATE THEN row_eff_begin_dttm END ASC,
                row_eff_end_dttm DESC
            ) = 1
        ),
        historical_bridge AS (
            SELECT
                c.cohort_month,
                snap.account_uri,
                snap.organization_uri AS org_uri
            FROM (SELECT DISTINCT cohort_month FROM WORKSPACE.digitalda_stage.Account_ID_features_monthly) c
            JOIN PREP.MDM_RELTIO.f_entity_wxaccountnumber_organization_snapshot snap
                ON c.cohort_month >= '2026-03-01'::DATE
                AND c.cohort_month >= snap.row_eff_begin_dttm
                AND (c.cohort_month < snap.row_eff_end_dttm OR snap.row_eff_end_dttm IS NULL)
            UNION ALL
            SELECT 
                c.cohort_month,
                a.account_uri,
                a.org_uri
            FROM (SELECT DISTINCT cohort_month FROM WORKSPACE.digitalda_stage.Account_ID_features_monthly) c
            CROSS JOIN anchor_records a
            WHERE c.cohort_month < '2026-03-01'::DATE
        )
        SELECT * FROM historical_bridge
    ) hb ON wx.uri = hb.account_uri AND af.cohort_month = hb.cohort_month
),

active_in_prev AS (
    SELECT 
        af.account_id AS ACCOUNT_ID,
        af.gallons_mth AS GALLONS_APR25,
        af.revenue_mth AS REVENUE_APR25,
        COALESCE(b.ORG_URI, 'UNMAPPED') AS ORG_URI
    FROM WORKSPACE.digitalda_stage.Account_ID_features_monthly af
    LEFT JOIN RELTIO_BRIDGE b ON af.account_id = b.ACCOUNT_ID AND af.cohort_month = b.COHORT_MONTH
    WHERE af.cohort_month = '2025-04-01'::DATE
      AND af.partner_ind = 'Wex'
),

entity_metrics_apr25 AS (
    SELECT 
        ORG_URI,
        SUM(GALLONS_APR25) AS ENT_GALLONS_APR25,
        SUM(REVENUE_APR25) AS ENT_REVENUE_APR25
    FROM active_in_prev
    GROUP BY ORG_URI
),

silent_entities_apr26 AS (
    SELECT 
        m.ORG_URI,
        m.HISTORICAL_ATTRITION_DECISION
    FROM WORKSPACE.digitalda_stage.ML_Attrition_Master_Table m
    WHERE m.cohort_month = '2026-04-01'::DATE
      AND m.HISTORICAL_ATTRITION_DECISION = 'State: Voluntary (Silent)'
)

SELECT 
    COUNT(DISTINCT s.ORG_URI) AS SILENT_ENTITIES_COUNT,
    SUM(e.ENT_GALLONS_APR25) AS GALLONS_APR25_MISSED_BY_ALAINA,
    SUM(e.ENT_REVENUE_APR25) AS REVENUE_APR25_MISSED_BY_ALAINA
FROM silent_entities_apr26 s
LEFT JOIN entity_metrics_apr25 e ON s.ORG_URI = e.ORG_URI;


-- ════════════════════════════════════════════════════════════════════
-- QUERY 3: INVOLUNTARY CHURN (CREDIT RISK / CHARGE-OFFS) IN ALAINA'S REPORT
-- Purpose: Of Alaina's reported closed accounts, how much represents involuntary attrition
--          (credit risk/bankruptcy/fraud closures), grouped by card range?
--          Involuntary churn should be separated from voluntary behavior in churn modeling.
-- ════════════════════════════════════════════════════════════════════

WITH 
RELTIO_BRIDGE AS (
    SELECT DISTINCT
        wx.accountnumber AS ACCOUNT_ID,
        hb.org_uri AS ORG_URI,
        hb.cohort_month AS COHORT_MONTH
    FROM WORKSPACE.digitalda_stage.Account_ID_features_monthly af
    JOIN PREP.MDM_RELTIO.entity_wxaccountnumber wx ON af.account_id = wx.accountnumber
    JOIN (
        WITH anchor_records AS (
            SELECT 
                account_uri,
                organization_uri AS org_uri
            FROM PREP.MDM_RELTIO.f_entity_wxaccountnumber_organization_snapshot
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY account_uri 
                ORDER BY
                    CASE WHEN row_eff_begin_dttm <= '2026-03-01'::DATE AND (row_eff_end_dttm IS NULL OR row_eff_end_dttm > '2026-03-01'::DATE) THEN 1
                    WHEN row_eff_begin_dttm > '2026-03-01'::DATE THEN 2
                    ELSE 3 END,
                CASE WHEN row_eff_begin_dttm > '2026-03-01'::DATE THEN row_eff_begin_dttm END ASC,
                row_eff_end_dttm DESC
            ) = 1
        ),
        historical_bridge AS (
            SELECT
                c.cohort_month,
                snap.account_uri,
                snap.organization_uri AS org_uri
            FROM (SELECT DISTINCT cohort_month FROM WORKSPACE.digitalda_stage.Account_ID_features_monthly) c
            JOIN PREP.MDM_RELTIO.f_entity_wxaccountnumber_organization_snapshot snap
                ON c.cohort_month >= '2026-03-01'::DATE
                AND c.cohort_month >= snap.row_eff_begin_dttm
                AND (c.cohort_month < snap.row_eff_end_dttm OR snap.row_eff_end_dttm IS NULL)
            UNION ALL
            SELECT 
                c.cohort_month,
                a.account_uri,
                a.org_uri
            FROM (SELECT DISTINCT cohort_month FROM WORKSPACE.digitalda_stage.Account_ID_features_monthly) c
            CROSS JOIN anchor_records a
            WHERE c.cohort_month < '2026-03-01'::DATE
        )
        SELECT * FROM historical_bridge
    ) hb ON wx.uri = hb.account_uri AND af.cohort_month = hb.cohort_month
),

ACCOUNT_CLOSED AS (
    SELECT 
        SOURCE_ACCOUNT_ID AS ACCOUNT_ID,
        MAX(CASE 
            WHEN ACCOUNT_CLOSED_DATE >= '3000-01-01'::DATE THEN NULL 
            ELSE ACCOUNT_CLOSED_DATE 
        END) AS CLOSED_DATE,
        MAX(ATTRITION_TYPE) AS ATTRITION_TYPE
    FROM FINANCE_ANALYTICS.NAM_PORTFOLIO_METRICS.NAM_ACCOUNT_ATTRITION
    GROUP BY SOURCE_ACCOUNT_ID
),

active_in_prev AS (
    SELECT 
        af.account_id AS ACCOUNT_ID,
        af.gallons_mth AS GALLONS_APR25,
        af.revenue_mth AS REVENUE_APR25,
        af.outstanding_cardcount_mth AS OUTSTANDING_CARDS_APR25,
        COALESCE(b.ORG_URI, 'UNMAPPED') AS ORG_URI,
        ac.CLOSED_DATE,
        ac.ATTRITION_TYPE
    FROM WORKSPACE.digitalda_stage.Account_ID_features_monthly af
    LEFT JOIN RELTIO_BRIDGE b ON af.account_id = b.ACCOUNT_ID AND af.cohort_month = b.COHORT_MONTH
    LEFT JOIN ACCOUNT_CLOSED ac ON af.account_id = ac.ACCOUNT_ID
    WHERE af.cohort_month = '2025-04-01'::DATE
      AND af.partner_ind = 'Wex'
),

alaina_closed_accounts AS (
    SELECT * FROM active_in_prev
    WHERE CLOSED_DATE > '2025-04-01'::DATE
      AND CLOSED_DATE <= '2026-04-30'::DATE
)

SELECT 
    CASE 
        WHEN COALESCE(OUTSTANDING_CARDS_APR25, 0) = 1 THEN '1'
        WHEN COALESCE(OUTSTANDING_CARDS_APR25, 0) = 2 THEN '2'
        WHEN COALESCE(OUTSTANDING_CARDS_APR25, 0) BETWEEN 3 AND 4 THEN '3-4'
        WHEN COALESCE(OUTSTANDING_CARDS_APR25, 0) BETWEEN 5 AND 9 THEN '5-9'
        WHEN COALESCE(OUTSTANDING_CARDS_APR25, 0) BETWEEN 10 AND 25 THEN '10-25'
        WHEN COALESCE(OUTSTANDING_CARDS_APR25, 0) BETWEEN 26 AND 50 THEN '26-50'
        WHEN COALESCE(OUTSTANDING_CARDS_APR25, 0) BETWEEN 51 AND 250 THEN '51-250'
        WHEN COALESCE(OUTSTANDING_CARDS_APR25, 0) BETWEEN 251 AND 1000 THEN '251-1000'
        WHEN COALESCE(OUTSTANDING_CARDS_APR25, 0) > 1000 THEN '1000+'
        ELSE 'Null cards'
    END AS CARD_RANGE,
    COUNT(DISTINCT CASE WHEN ATTRITION_TYPE = 'Involuntary' THEN ACCOUNT_ID END) AS INVOLUNTARY_ACCOUNTS_COUNT,
    SUM(CASE WHEN ATTRITION_TYPE = 'Involuntary' THEN GALLONS_APR25 ELSE 0 END) AS INVOLUNTARY_GALLONS,
    SUM(CASE WHEN ATTRITION_TYPE = 'Involuntary' THEN REVENUE_APR25 ELSE 0 END) AS INVOLUNTARY_REVENUE
FROM alaina_closed_accounts
GROUP BY CARD_RANGE
ORDER BY CARD_RANGE;


-- ════════════════════════════════════════════════════════════════════
-- QUERY 4: ENTITY-LEVEL REPLICATION OF MOR ATTRITION DATA
-- Purpose: Replicates Alaina & Rachel's MOR Attrition report format
--          but at the Business Entity level (ORG_URI) using the pipeline's
--          stabilized tables (C3 & C4) as the source of truth.
-- ════════════════════════════════════════════════════════════════════

WITH 
-- 1. Active Entities and their metrics in the base month (April 2025)
base_active_entities AS (
    SELECT 
        org_uri,
        ent_fleet_cards AS fleet_cards_apr25,
        ent_revenue AS revenue_apr25,
        ent_gallons AS gallons_apr25
    FROM WORKSPACE.digitalda_stage.Enterprise_Entity_Features_Rolling
    WHERE cohort_month = '2025-04-01'::DATE
      AND partner_ind = 'Wex' -- align with MOR exclusions
),

-- 2. Entity Status in the evaluation month (April 2026)
evaluation_status AS (
    SELECT 
        org_uri,
        HISTORICAL_ATTRITION_DECISION
    FROM WORKSPACE.digitalda_stage.ML_Attrition_Master_Table
    WHERE cohort_month = '2026-04-01'::DATE
),

-- 3. Combine base metrics and evaluation status
entity_yoy_metrics AS (
    SELECT 
        b.org_uri,
        b.fleet_cards_apr25,
        b.revenue_apr25,
        b.gallons_apr25,
        COALESCE(e.HISTORICAL_ATTRITION_DECISION, 'Healthy') AS attrition_decision,
        
        -- Segment grouping based on Entity's outstanding cards in April 2025
        CASE 
            WHEN COALESCE(b.fleet_cards_apr25, 0) = 1 THEN '1'
            WHEN COALESCE(b.fleet_cards_apr25, 0) = 2 THEN '2'
            WHEN COALESCE(b.fleet_cards_apr25, 0) BETWEEN 3 AND 4 THEN '3-4'
            WHEN COALESCE(b.fleet_cards_apr25, 0) BETWEEN 5 AND 9 THEN '5-9'
            WHEN COALESCE(b.fleet_cards_apr25, 0) BETWEEN 10 AND 25 THEN '10-25'
            WHEN COALESCE(b.fleet_cards_apr25, 0) BETWEEN 26 AND 50 THEN '26-50'
            WHEN COALESCE(b.fleet_cards_apr25, 0) BETWEEN 51 AND 250 THEN '51-250'
            WHEN COALESCE(b.fleet_cards_apr25, 0) BETWEEN 251 AND 1000 THEN '251-1000'
            WHEN COALESCE(b.fleet_cards_apr25, 0) > 1000 THEN '1000+'
            ELSE 'Null cards'
        END AS fleet_segment,

        -- Attrition flags
        CASE WHEN e.HISTORICAL_ATTRITION_DECISION IN ('Event: Voluntary (Hard Close)', 'State: Voluntary (Silent)') THEN 1 ELSE 0 END AS is_voluntary_attrition,
        CASE WHEN e.HISTORICAL_ATTRITION_DECISION = 'Event: Risk/Involuntary' THEN 1 ELSE 0 END AS is_involuntary_attrition
    FROM base_active_entities b
    LEFT JOIN evaluation_status e ON b.org_uri = e.org_uri
)

-- 4. Aggregate by Fleet Segment to replicate Alaina's layout at the Entity Level
SELECT 
    fleet_segment AS "Fleet Size",
    
    -- Active Entity count in Apr 2025
    COUNT(DISTINCT org_uri) AS "Active Entities (Apr 2025)",
    
    -- Voluntary Entity Churn
    COUNT(DISTINCT CASE WHEN is_voluntary_attrition = 1 THEN org_uri END) AS "Entity Attrition Count (Apr 2026)",
    SUM(CASE WHEN is_voluntary_attrition = 1 THEN revenue_apr25 ELSE 0 END) AS "Revenue Attrition ($)",
    SUM(CASE WHEN is_voluntary_attrition = 1 THEN gallons_apr25 ELSE 0 END) AS "Gallon Attrition",
    
    -- Involuntary Entity Churn (Credit/Charge-offs)
    COUNT(DISTINCT CASE WHEN is_involuntary_attrition = 1 THEN org_uri END) AS "Involuntary Entity Count",
    SUM(CASE WHEN is_involuntary_attrition = 1 THEN revenue_apr25 ELSE 0 END) AS "Involuntary Revenue ($)",
    SUM(CASE WHEN is_involuntary_attrition = 1 THEN gallons_apr25 ELSE 0 END) AS "Involuntary Gallons"

FROM entity_yoy_metrics
GROUP BY fleet_segment
ORDER BY 
    CASE 
        WHEN fleet_segment = '1' THEN 1
        WHEN fleet_segment = '2' THEN 2
        WHEN fleet_segment = '3-4' THEN 3
        WHEN fleet_segment = '5-9' THEN 4
        WHEN fleet_segment = '10-25' THEN 5
        WHEN fleet_segment = '26-50' THEN 6
        WHEN fleet_segment = '51-250' THEN 7
        WHEN fleet_segment = '251-1000' THEN 8
        WHEN fleet_segment = '1000+' THEN 9
        ELSE 10 
    END;
