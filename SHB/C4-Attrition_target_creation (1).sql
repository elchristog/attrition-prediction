-- ════════════════════════════════════════════════════════════════════════════
-- C4 — Entity_ID_target_table
-- Grain : ORG_URI × cohort_month
-- Purpose: Add attrition status + 4 forward-looking targets + exclusion flags
-- Sources : Entity_ID_features_monthly (C3)
--           entity_Christian_target_variable (C1)
-- ──────────────────────────────────────────────────────────────────────────
-- EXCLUSION FLAGS
--   excl_flag_1  = 1  when the attrition decision is a "noise" class
--                     (Involuntary / Graveyard / Flip-Conversion).
--                     Use WHERE excl_flag_1 = 0 to keep only Healthy +
--                     Voluntary (Hard Close + Silent) rows.
--
--   excl_flag_2  = 1  when cohort_month is AFTER the entity's first-ever
--                     attrition_date. Prevents leaking post-attrition rows
--                     into training.
--
--   excl_flag_3_6m   = 1  when EITHER:
--                          (a) cohort_month is strictly AFTER the entity's
--                              first attrition_date  — i.e. rows after the
--                              actual attrition event are excluded, but the
--                              attrition month itself is KEPT so the label
--                              is available for training, OR
--                          (b) cohort_month does not have 6 full future
--                              months available as of today (label unknowable).
--   excl_flag_3_8m   = same logic for the 8-month target.
--   excl_flag_3_12m  = same logic for the 12-month target.
--
--   NOTE: excl_flag_3 no longer cuts at the first month the TARGET window
--         opened. It now aligns with the actual attrition date, keeping all
--         cohort rows up to and including the attrition month, and only
--         dropping rows after that date.
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE WORKSPACE.digitalda_stage.Entity_ID_target_table AS

WITH status_classified AS (
    -- Binary-encode the attrition decision for forward-looking target math
    SELECT
        ORG_URI,
        EVALUATION_MONTH,
        ACCOUNT_COUNT,
        ACTIVE_COUNT,
        HISTORICAL_ATTRITION_DECISION,
        FLIP_DATE,
        ATTRITION_DATE,
        CASE
            WHEN HISTORICAL_ATTRITION_DECISION IN (
                'Event: Voluntary (Hard Close)',
                'State: Voluntary (Silent)'
            ) THEN 1
            WHEN HISTORICAL_ATTRITION_DECISION IN (
                'Healthy',
                'Inactive (Graveyard)',
                'Event: Risk/Involuntary',
                'Event: Flip/Conversion'
            ) THEN 0
            ELSE NULL   -- unexpected values → NULL (investigate)
        END AS is_attrition_event
    FROM WORKSPACE.digitalda_stage.entity_Christian_target_variable
),

-- ── Earliest attrition date per entity (drives excl_flag_2 AND excl_flag_3) 
entity_first_attrition AS (
    SELECT
        ORG_URI,
        MIN(ATTRITION_DATE) AS first_attrition_date
    FROM WORKSPACE.digitalda_stage.entity_Christian_target_variable
    WHERE ATTRITION_DATE IS NOT NULL
    GROUP BY ORG_URI
),

-- ── Forward-looking target variables ──────────────────────────────────────
entity_future_attrition AS (
    SELECT
        a.ORG_URI,
        a.cohort_month,

        -- STATUS (current month join)
        s.HISTORICAL_ATTRITION_DECISION,
        s.is_attrition_event                                AS target_current_month,
        s.FLIP_DATE,
        s.ATTRITION_DATE,
        s.ACCOUNT_COUNT                                     AS status_account_count,
        s.ACTIVE_COUNT                                      AS status_active_count,

        -- TARGET: attrition in next 3 months (cohort_month+1 … cohort_month+3)
        MAX(CASE
            WHEN f3.is_attrition_event = 1
             AND DATE_TRUNC('month', f3.EVALUATION_MONTH) >  a.cohort_month
             AND DATE_TRUNC('month', f3.EVALUATION_MONTH) <= DATEADD('month', 3, a.cohort_month)
            THEN 1 ELSE 0
        END)                                                AS target_3m,

        -- TARGET: attrition in next 6 months (cohort_month+1 … cohort_month+6)
        MAX(CASE
            WHEN f6.is_attrition_event = 1
             AND DATE_TRUNC('month', f6.EVALUATION_MONTH) >  a.cohort_month
             AND DATE_TRUNC('month', f6.EVALUATION_MONTH) <= DATEADD('month', 6, a.cohort_month)
            THEN 1 ELSE 0
        END)                                                AS target_6m,

        -- TARGET: attrition in next 8 months
        MAX(CASE
            WHEN f8.is_attrition_event = 1
             AND DATE_TRUNC('month', f8.EVALUATION_MONTH) >  a.cohort_month
             AND DATE_TRUNC('month', f8.EVALUATION_MONTH) <= DATEADD('month', 8, a.cohort_month)
            THEN 1 ELSE 0
        END)                                                AS target_8m,

        -- TARGET: attrition in next 12 months
        MAX(CASE
            WHEN f12.is_attrition_event = 1
             AND DATE_TRUNC('month', f12.EVALUATION_MONTH) >  a.cohort_month
             AND DATE_TRUNC('month', f12.EVALUATION_MONTH) <= DATEADD('month', 12, a.cohort_month)
            THEN 1 ELSE 0
        END)                                                AS target_12m

    FROM WORKSPACE.digitalda_stage.Entity_ID_features_monthly a

    -- Current-month status
    LEFT JOIN status_classified s
        ON  s.ORG_URI                               = a.ORG_URI
        AND DATE_TRUNC('month', s.EVALUATION_MONTH)  = a.cohort_month

    -- Forward joins (separate aliases so window ranges are independent)
    LEFT JOIN status_classified f3
        ON  f3.ORG_URI                               = a.ORG_URI
        AND DATE_TRUNC('month', f3.EVALUATION_MONTH)  >  a.cohort_month
        AND DATE_TRUNC('month', f3.EVALUATION_MONTH)  <= DATEADD('month', 3, a.cohort_month)

    LEFT JOIN status_classified f6
        ON  f6.ORG_URI                               = a.ORG_URI
        AND DATE_TRUNC('month', f6.EVALUATION_MONTH)  >  a.cohort_month
        AND DATE_TRUNC('month', f6.EVALUATION_MONTH)  <= DATEADD('month', 6, a.cohort_month)

    LEFT JOIN status_classified f8
        ON  f8.ORG_URI                               = a.ORG_URI
        AND DATE_TRUNC('month', f8.EVALUATION_MONTH)  >  a.cohort_month
        AND DATE_TRUNC('month', f8.EVALUATION_MONTH)  <= DATEADD('month', 8, a.cohort_month)

    LEFT JOIN status_classified f12
        ON  f12.ORG_URI                              = a.ORG_URI
        AND DATE_TRUNC('month', f12.EVALUATION_MONTH) >  a.cohort_month
        AND DATE_TRUNC('month', f12.EVALUATION_MONTH) <= DATEADD('month', 12, a.cohort_month)

    GROUP BY ALL
)

-- ════════════════════════════════════════════════════════════════════════════
-- FINAL OUTPUT
-- ════════════════════════════════════════════════════════════════════════════
SELECT
    -- ── GRAIN ──
    e.ORG_URI,
    e.cohort_month,

    -- ── ATTRITION STATUS (current month) ──
    e.HISTORICAL_ATTRITION_DECISION,
    e.target_current_month,
    e.FLIP_DATE,
    e.ATTRITION_DATE,
    e.status_account_count,
    e.status_active_count,

    -- ── FORWARD-LOOKING TARGETS ──
    e.target_3m,
    e.target_6m,
    e.target_8m,
    e.target_12m,

    -- ════════════════════════════════════════════════════════
    -- EXCLUSION FLAGS
    -- ════════════════════════════════════════════════════════

    -- ── excl_flag_1: NOISE-CLASS FILTER ──────────────────────────────────
    -- 1 = row belongs to an involuntary / graveyard / flip class.
    CASE
        WHEN e.HISTORICAL_ATTRITION_DECISION IN (
            'Event: Risk/Involuntary',
            'Inactive (Graveyard)',
            'Event: Flip/Conversion'
        ) THEN 1
        ELSE 0
    END                                                     AS excl_flag_1,

    -- ── excl_flag_2: POST-ATTRITION ROW FILTER ───────────────────────────
    -- 1 = cohort_month is strictly AFTER the entity's earliest attrition_date.
    CASE
        WHEN fa.first_attrition_date IS NOT NULL
         AND e.cohort_month > DATE_TRUNC('month', fa.first_attrition_date)
        THEN 1
        ELSE 0
    END                                                     AS excl_flag_2,

    -- ── excl_flag_3_6m: TARGET-6M HORIZON FILTER ─────────────────────────
    -- 1 when EITHER:
    --   (a) cohort_month is strictly AFTER the entity's first attrition_date.
    --       The attrition month itself (cohort_month = attrition month) is
    --       KEPT so the positive label is present in training data.
    --       Only the months that follow the attrition event are dropped.
    --   (b) cohort_month does not have 6 full future months available
    --       as of today — the label cannot be reliably assigned.
    CASE
        WHEN (fa.first_attrition_date IS NOT NULL
              AND e.cohort_month > DATE_TRUNC('month', fa.first_attrition_date))
          OR e.cohort_month > DATEADD('month', -6, DATE_TRUNC('month', CURRENT_DATE()))
        THEN 1
        ELSE 0
    END                                                     AS excl_flag_3_6m,

    -- ── excl_flag_3_8m: TARGET-8M HORIZON FILTER ─────────────────────────
    CASE
        WHEN (fa.first_attrition_date IS NOT NULL
              AND e.cohort_month > DATE_TRUNC('month', fa.first_attrition_date))
          OR e.cohort_month > DATEADD('month', -8, DATE_TRUNC('month', CURRENT_DATE()))
        THEN 1
        ELSE 0
    END                                                     AS excl_flag_3_8m,

    -- ── excl_flag_3_12m: TARGET-12M HORIZON FILTER ───────────────────────
    CASE
        WHEN (fa.first_attrition_date IS NOT NULL
              AND e.cohort_month > DATE_TRUNC('month', fa.first_attrition_date))
          OR e.cohort_month > DATEADD('month', -12, DATE_TRUNC('month', CURRENT_DATE()))
        THEN 1
        ELSE 0
    END                                                     AS excl_flag_3_12m,

    -- ── ALL FEATURE COLUMNS FROM Entity_ID_features_monthly (C3) ──
    a.* EXCLUDE (
        org_uri,
        cohort_month,
        any_closed_next_3m,
        any_closed_next_6m,
        any_closed_next_8m,
        any_closed_next_12m
    )

FROM entity_future_attrition e

-- Feature table (C3)
LEFT JOIN WORKSPACE.digitalda_stage.Entity_ID_features_monthly a
    ON  a.ORG_URI      = e.ORG_URI
    AND a.cohort_month = DATE_TRUNC('month', e.cohort_month)

-- First attrition date (drives both excl_flag_2 and excl_flag_3_*)
LEFT JOIN entity_first_attrition fa
    ON  fa.ORG_URI = e.ORG_URI

ORDER BY e.ORG_URI, e.cohort_month
;

-- ════════════════════════════════════════════════════════════════════════════
-- VALIDATION QUERIES
-- ════════════════════════════════════════════════════════════════════════════

-- Full sample
SELECT * FROM WORKSPACE.digitalda_stage.Entity_ID_target_table LIMIT 1000;

-- Class distribution (excluding current month)
SELECT
    historical_attrition_decision,
    COUNT(DISTINCT org_uri) AS entity_count
FROM WORKSPACE.digitalda_stage.Entity_ID_target_table
WHERE cohort_month < '2026-04-01'
GROUP BY ALL;

-- Exclusion flag summary (use to understand data loss per flag)
SELECT
    excl_flag_1,
    excl_flag_2,
    excl_flag_3_6m,
    excl_flag_3_8m,
    excl_flag_3_12m,
    COUNT(*)                      AS row_count,
    COUNT(DISTINCT org_uri)       AS entity_count
FROM WORKSPACE.digitalda_stage.Entity_ID_target_table
WHERE cohort_month < '2026-04-01'
GROUP BY ALL
ORDER BY ALL;

-- Clean training set for 3m target (apply all three exclusions)
-- SELECT * FROM WORKSPACE.digitalda_stage.Entity_ID_target_table
-- WHERE excl_flag_1    = 0
--   AND excl_flag_2    = 0
--   AND excl_flag_3_3m = 0
--   AND cohort_month   < '2026-04-01';

-- Clean training set for 6m target
-- SELECT * FROM WORKSPACE.digitalda_stage.Entity_ID_target_table
-- WHERE excl_flag_1    = 0
--   AND excl_flag_2    = 0
--   AND excl_flag_3_6m = 0
--   AND cohort_month   < '2026-04-01';

-- Clean training set for 8m target
-- SELECT * FROM WORKSPACE.digitalda_stage.Entity_ID_target_table
-- WHERE excl_flag_1    = 0
--   AND excl_flag_2    = 0
--   AND excl_flag_3_8m = 0
--   AND cohort_month   < '2026-04-01';

-- Clean training set for 12m target
-- SELECT * FROM WORKSPACE.digitalda_stage.Entity_ID_target_table
-- WHERE excl_flag_1     = 0
--   AND excl_flag_2     = 0
--   AND excl_flag_3_12m = 0
--   AND cohort_month    < '2026-04-01';

SELECT * FROM WORKSPACE.digitalda_stage.Entity_ID_target_table LIMIT 10;