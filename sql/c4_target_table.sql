-- ════════════════════════════════════════════════════════════════════════════
-- C4 — ML_Attrition_Master_Table
-- Grain : ORG_URI × cohort_month
-- Purpose: Add attrition status + 4 forward-looking targets + exclusion flags
-- Sources : Enterprise_Entity_Features_Rolling (C3)
--           entity_Christian_target_variable (C1)
-- ──────────────────────────────────────────────────────────────────────────
-- EXCLUSION FLAGS
--   excl_flag_1  = 1  when the attrition decision is a "noise" class
--                     (Involuntary / Graveyard / Flip-Conversion).
--                     Use WHERE excl_flag_1 = 0 to keep only Healthy +
--                     Voluntary (Hard Close + Silent) rows.
--
--   excl_flag_2  = 1  when cohort_month is AFTER the entity's first-ever
--                     attrition_date.  Prevents leaking post-attrition rows
--                     into training.
--
--   excl_flag_3_3m   = 1  when (a) cohort_month falls AFTER the first month
--                          where target_3m became TRUE, OR (b) the entity
--                          does not have 3 full future months available as
--                          of today (so the label is unknowable).
--   excl_flag_3_6m   = same logic for the 6-month target.
--   excl_flag_3_8m   = same logic for the 8-month target.
--   excl_flag_3_12m  = same logic for the 12-month target.
--   Use the appropriate flag depending on which target variable you train on.
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE WORKSPACE.digitalda_stage.ML_Attrition_Master_Table AS

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
        END AS is_attrition_event,
        CASE
            WHEN HISTORICAL_ATTRITION_DECISION = 'Event: Voluntary (Hard Close)' THEN 1
            ELSE 0
        END AS is_hard_event,
        CASE
            WHEN HISTORICAL_ATTRITION_DECISION = 'State: Voluntary (Silent)' THEN 1
            ELSE 0
        END AS is_silent_event
    FROM WORKSPACE.digitalda_stage.entity_Christian_target_variable
),

-- ── Earliest attrition date per entity (for excl_flag_2) ──────────────────
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
        a.org_uri,
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
        END)                                                AS target_12m,

        -- SEGMENTED TARGETS (8M Horizon as baseline)
        MAX(CASE
            WHEN f8.is_hard_event = 1
             AND DATE_TRUNC('month', f8.EVALUATION_MONTH) >  a.cohort_month
             AND DATE_TRUNC('month', f8.EVALUATION_MONTH) <= DATEADD('month', 8, a.cohort_month)
            THEN 1 ELSE 0
        END)                                                AS target_hard_8m,

        MAX(CASE
            WHEN f8.is_silent_event = 1
             AND DATE_TRUNC('month', f8.EVALUATION_MONTH) >  a.cohort_month
             AND DATE_TRUNC('month', f8.EVALUATION_MONTH) <= DATEADD('month', 8, a.cohort_month)
            THEN 1 ELSE 0
        END)                                                AS target_silent_8m

    FROM WORKSPACE.digitalda_stage.Enterprise_Entity_Features_Rolling a

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

    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
)

-- ════════════════════════════════════════════════════════════════════════════
-- FINAL OUTPUT
-- ════════════════════════════════════════════════════════════════════════════
SELECT
    -- ── GRAIN ──
    e.org_uri,
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
    e.target_hard_8m,
    e.target_silent_8m,

    -- ════════════════════════════════════════════════════════
    -- EXCLUSION FLAGS
    -- ════════════════════════════════════════════════════════

    -- ── excl_flag_1: NOISE-CLASS FILTER ──────────────────────────────────
    -- 1 = row belongs to an involuntary / graveyard / flip class.
    -- These are excluded from voluntary-attrition modelling to avoid
    -- label contamination.  Set to 0 for Healthy and Voluntary rows.
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
    -- These rows represent the "zombie" months after a customer has already
    -- left and should not be used in model training.
    CASE
        WHEN fa.first_attrition_date IS NOT NULL
         AND e.cohort_month > DATE_TRUNC('month', fa.first_attrition_date)
        THEN 1
        ELSE 0
    END                                                     AS excl_flag_2,

    -- ── excl_flag_3_3m: TARGET-3M HORIZON FILTER ─────────────────────────
    -- 1 when EITHER:
    --   (a) cohort_month > the first month the 3m target was TRUE
    --       (entity has already attrited; later rows are look-ahead leakage)
    --   (b) cohort_month > DATE_TRUNC('month', CURRENT_DATE) - 3 months
    --       (not enough future data exists to label this row)
    CASE
        WHEN e.cohort_month > DATEADD('month', -3, DATE_TRUNC('month', CURRENT_DATE()))
        THEN 1
        ELSE 0
    END                                                     AS excl_flag_3_3m,

    -- ── excl_flag_3_6m: TARGET-6M HORIZON FILTER ─────────────────────────
    CASE
        WHEN e.cohort_month > DATEADD('month', -6, DATE_TRUNC('month', CURRENT_DATE()))
        THEN 1
        ELSE 0
    END                                                     AS excl_flag_3_6m,

    -- ── excl_flag_3_8m: TARGET-8M HORIZON FILTER ─────────────────────────
    CASE
        WHEN e.cohort_month > DATEADD('month', -8, DATE_TRUNC('month', CURRENT_DATE()))
        THEN 1
        ELSE 0
    END                                                     AS excl_flag_3_8m,

    -- ── excl_flag_3_12m: TARGET-12M HORIZON FILTER ───────────────────────
    CASE
        WHEN e.cohort_month > DATEADD('month', -12, DATE_TRUNC('month', CURRENT_DATE()))
        THEN 1
        ELSE 0
    END                                                     AS excl_flag_3_12m,

    -- ── ALL FEATURE COLUMNS FROM Enterprise_Entity_Features_Rolling (C3) ──
    a.* EXCLUDE (
        org_uri,
        cohort_month
    )

FROM entity_future_attrition e

-- Feature table (C3)
LEFT JOIN WORKSPACE.digitalda_stage.Enterprise_Entity_Features_Rolling a
    ON  a.ORG_URI      = e.ORG_URI
    AND a.cohort_month = DATE_TRUNC('month', e.cohort_month)

-- First attrition date (for excl_flag_2)
LEFT JOIN entity_first_attrition fa
    ON  fa.ORG_URI = e.ORG_URI


ORDER BY e.ORG_URI, e.cohort_month
;
