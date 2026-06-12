-- ====================================================================
-- ENTITY-LEVEL REPLICATION OF MOR ATTRITION DATA
-- Purpose: Replicates Alaina & Rachel's MOR Attrition report format
--          but at the Business Entity level (ORG_URI) using the pipeline's
--          stabilized tables (C3 & C4) as the source of truth.
-- Date: 2026-06-12
-- ====================================================================

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
