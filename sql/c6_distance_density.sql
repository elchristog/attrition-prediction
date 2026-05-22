-- ════════════════════════════════════════════════════════════════════════════
-- C6 — ACCOUNT_DISTANCE_DENSITY_C6
-- Grain : ORG_URI / accountnumber
-- Purpose: Calculate distance to closest sites, density of nearby sites, 
--          and classify the geographic footprint of accounts based on H3 Spatial Analysis.
-- ════════════════════════════════════════════════════════════════════════════

-- ============================================================================
-- Step 1: Set Date Variables for Transaction lookback
-- ============================================================================
-- We use Snowflake variables to scope the transaction lookback dynamically
SET report_date = CURRENT_DATE();
SET end_date = LAST_DAY($report_date - INTERVAL '1 month');
SET start_date = DATE_TRUNC('month', $end_date) - INTERVAL '5 months';

-- ============================================================================
-- Step 2: Site Locations & H3 Indexing
-- ============================================================================
CREATE OR REPLACE TEMPORARY TABLE WORKSPACE.digitalda_stage.tmp_site_geo AS
SELECT DISTINCT
    pos.source_merchant_site_id,
    TRY_TO_DOUBLE(pos.site_longitude) AS site_longitude,
    TRY_TO_DOUBLE(pos.site_latitude) AS site_latitude,
    ST_MAKEPOINT(TRY_TO_DOUBLE(pos.site_longitude), TRY_TO_DOUBLE(pos.site_latitude)) AS site_point,
    H3_POINT_TO_CELL(ST_MAKEPOINT(TRY_TO_DOUBLE(pos.site_longitude), TRY_TO_DOUBLE(pos.site_latitude)), 7) AS site_res7_cell,
    H3_POINT_TO_CELL(ST_MAKEPOINT(TRY_TO_DOUBLE(pos.site_longitude), TRY_TO_DOUBLE(pos.site_latitude)), 3) AS site_res3_cell
FROM GLOBAL_FLEET_ANALYTICS.EDW_OWNER.D_POS_AND_SITE pos
JOIN WORKSPACE.salesmktg.partner_site_network psn USING(source_merchant_site_id)
WHERE psn.current_month_flag = 1
  AND pos.current_record_flg = '1'
  AND TRY_TO_DOUBLE(pos.site_latitude) BETWEEN -90 AND 90
  AND TRY_TO_DOUBLE(pos.site_longitude) BETWEEN -180 AND 180;

-- ============================================================================
-- Step 3: Raw Transactions
-- ============================================================================
CREATE OR REPLACE TEMPORARY TABLE WORKSPACE.digitalda_stage.tmp_transactions_raw AS
SELECT
    tl.purchase_device_key,
    ac.source_account_id AS accountnumber,
    p.marketing_partner_nm AS acct_marketing_partner_nm,
    IFF(tl.inn_purchase_gallons_qty <> 0, 'INN', 'OON') AS transaction_type,
    sg.source_merchant_site_id,
    COUNT(DISTINCT wex_transaction_id) AS transaction_count
FROM GLOBAL_FLEET_ANALYTICS.EDW_OWNER.F_TRANSACTION_LINE_ITEM_VW tl
INNER JOIN GLOBAL_FLEET_ANALYTICS.EDW_OWNER.D_POSTING_DATE_VW pd ON tl.posting_date_key = pd.post_date_key
INNER JOIN GLOBAL_FLEET_ANALYTICS.EDW_OWNER.D_PROGRAM p ON p.program_key = tl.program_key
INNER JOIN GLOBAL_FLEET_ANALYTICS.EDW_OWNER.D_POS_AND_SITE pos ON tl.pos_and_site_key = pos.pos_and_site_key
INNER JOIN WORKSPACE.digitalda_stage.tmp_site_geo sg ON pos.source_merchant_site_id = sg.source_merchant_site_id
INNER JOIN GLOBAL_FLEET_ANALYTICS.EDW_OWNER.D_ACCOUNT_HIST_VW ah ON tl.purchase_account_key = ah.account_key
INNER JOIN GLOBAL_FLEET_ANALYTICS.EDW_OWNER.D_ACCOUNT_CURRENT_VW ac ON ah.account_hist_key = ac.account_hist_key
WHERE post_calendar_date BETWEEN $start_date AND $end_date
  AND tl.purchase_gallons_qty <> 0
GROUP BY 1, 2, 3, 4, 5;

-- ============================================================================
-- Step 4: Account Heatmap
-- ============================================================================
CREATE OR REPLACE TEMPORARY TABLE WORKSPACE.digitalda_stage.tmp_account_heatmap AS
SELECT
    t.accountnumber,
    H3_CELL_TO_PARENT(sg.site_res7_cell, 4) AS h3_heatmap_parent_res4,
    sg.site_res7_cell AS h3_heatmap_cell,
    SUM(t.transaction_count) AS total_transactions
FROM WORKSPACE.digitalda_stage.tmp_transactions_raw t
JOIN WORKSPACE.digitalda_stage.tmp_site_geo sg ON t.source_merchant_site_id = sg.source_merchant_site_id
GROUP BY 1, 2, 3;

-- ============================================================================
-- Step 5: Account Addresses
-- ============================================================================
CREATE OR REPLACE TEMPORARY TABLE WORKSPACE.digitalda_stage.tmp_siebel_locations AS
SELECT
    org.loc,
    IFF(addr.latitude < 0, addr.longitude, addr.latitude) AS latitude,
    IFF(addr.latitude > 0, addr.longitude, addr.latitude) AS longitude,
    CASE
        WHEN IFF(addr.latitude < 0, addr.longitude, addr.latitude) BETWEEN -90 AND 90
         AND IFF(addr.longitude > 0, addr.latitude, addr.longitude) BETWEEN -180 AND 180
        THEN ST_MAKEPOINT(IFF(addr.longitude > 0, addr.latitude, addr.longitude), IFF(addr.latitude < 0, addr.longitude, addr.latitude))
    END AS siebel_point
FROM PREP.SBL__SIEBEL.S_ORG_EXT org
JOIN PREP.SBL__SIEBEL.S_ADDR_PER addr ON org.pr_addr_id = addr.row_id
JOIN GLOBAL_FLEET_ANALYTICS.EDW_OWNER.D_ACCOUNT_CURRENT_VW ac ON org.loc = ac.source_account_id
WHERE addr.latitude IS NOT NULL AND addr.longitude IS NOT NULL AND addr.latitude + addr.longitude <> 0;

CREATE OR REPLACE TEMPORARY TABLE WORKSPACE.digitalda_stage.tmp_account_points AS
WITH primary_points AS (
    SELECT DISTINCT
        ao.org_uri,
        an.accountnumber,
        COALESCE(ST_MAKEPOINT(aloc.longitude, aloc.latitude), sl.siebel_point, ST_MAKEPOINT(oloc.longitude, oloc.latitude)) AS raw_account_point,
        COALESCE(aloc.postalcode, oloc.postalcode) AS postalcode
    FROM PREP.MDM_RELTIO.ENTITY_WXACCOUNTNUMBER an
    JOIN PREP.MDM_RELTIO.ENTITY_WXACCOUNTNUMBER_ORGANIZATION ao ON an.uri = ao.uri
    JOIN PREP.MDM_RELTIO.ENTITY_WXACCOUNTNUMBER_ADDRESS ana ON an.uri = ana.accountnumber_uri
    JOIN PREP.MDM_RELTIO.ENTITY_LOCATION aloc ON ana.address_uri = aloc.address_id
    JOIN PREP.MDM_RELTIO.ENTITY_ORGANIZATION_ADDRESS oa ON oa.organization_uri = ao.org_uri
    JOIN PREP.MDM_RELTIO.ENTITY_LOCATION oloc ON oa.address_uri = oloc.address_id
    LEFT JOIN WORKSPACE.digitalda_stage.tmp_siebel_locations sl ON sl.loc = an.accountnumber
    WHERE accountplatform IN ('SIEBEL', 'TANDEM') AND an.active = TRUE
),
point_validation AS (
    SELECT
        pp.org_uri,
        pp.accountnumber,
        pp.raw_account_point,
        ST_MAKEPOINT(TRY_TO_DOUBLE(cbsa.zip_long), TRY_TO_DOUBLE(cbsa.zip_lat)) AS zip_centroid_point
    FROM primary_points pp
    LEFT JOIN WORKSPACE.salesmktg.cbsa_geocode_ds cbsa ON cbsa.zip = LEFT(pp.postalcode, 5)
)
SELECT
    org_uri,
    accountnumber,
    CASE
        WHEN raw_account_point IS NULL THEN zip_centroid_point
        WHEN zip_centroid_point IS NULL THEN raw_account_point
        WHEN ST_DISTANCE(raw_account_point, zip_centroid_point) / 1609.34 > 40 THEN zip_centroid_point
        ELSE raw_account_point
    END AS account_point
FROM point_validation;

-- ============================================================================
-- Step 6: Match Addresses to Heatmap
-- ============================================================================
CREATE OR REPLACE TEMPORARY TABLE WORKSPACE.digitalda_stage.tmp_best_account_address AS
WITH expanded_address_mapping AS (
    SELECT
        ap.org_uri,
        ap.accountnumber,
        ap.account_point,
        f.value::STRING AS search_res4_cell
    FROM WORKSPACE.digitalda_stage.tmp_account_points ap,
    LATERAL FLATTEN(input => H3_GRID_DISK(H3_POINT_TO_CELL(ap.account_point, 4), 2)) f
),
mapped_addresses AS (
    SELECT
        ea.org_uri,
        ea.accountnumber,
        ANY_VALUE(ea.account_point) AS account_point,
        SUM(h.total_transactions) AS total_transactions
    FROM expanded_address_mapping ea
    LEFT JOIN WORKSPACE.digitalda_stage.tmp_account_heatmap h ON ea.accountnumber = h.accountnumber AND ea.search_res4_cell = h.h3_heatmap_parent_res4
    GROUP BY 1, 2
)
SELECT * FROM mapped_addresses QUALIFY ROW_NUMBER() OVER(PARTITION BY accountnumber ORDER BY total_transactions DESC NULLS LAST) = 1;

-- ============================================================================
-- Step 7: Closest Site & Mileage Radii
-- ============================================================================
CREATE OR REPLACE TEMPORARY TABLE WORKSPACE.digitalda_stage.tmp_address_site_metrics AS
WITH expanded_search_areas AS (
    SELECT
        baa.org_uri,
        baa.accountnumber,
        baa.account_point,
        baa.total_transactions,
        f.value::STRING AS search_res3_cell
    FROM WORKSPACE.digitalda_stage.tmp_best_account_address baa,
    LATERAL FLATTEN(input => H3_GRID_DISK(H3_POINT_TO_CELL(baa.account_point, 3), 1)) f
    WHERE EXISTS (SELECT 1 FROM WORKSPACE.digitalda_stage.tmp_transactions_raw t WHERE t.accountnumber = baa.accountnumber)
),
coarse_filter AS (
    SELECT
        esa.org_uri,
        esa.accountnumber,
        esa.total_transactions,
        sg.source_merchant_site_id,
        ST_DISTANCE(esa.account_point, sg.site_point) / 1609.34 AS distance_miles,
        acct_partner.transaction_type,
        acct_partner.acct_marketing_partner_nm
    FROM expanded_search_areas esa
    JOIN WORKSPACE.digitalda_stage.tmp_site_geo sg ON esa.search_res3_cell = sg.site_res3_cell
    LEFT JOIN (
        SELECT DISTINCT accountnumber, acct_marketing_partner_nm, transaction_type, source_merchant_site_id 
        FROM WORKSPACE.digitalda_stage.tmp_transactions_raw 
        JOIN WORKSPACE.salesmktg.partner_site_network psn USING(source_merchant_site_id) 
        WHERE current_month_flag = 1
    ) acct_partner ON esa.accountnumber = acct_partner.accountnumber AND sg.source_merchant_site_id = acct_partner.source_merchant_site_id
    WHERE acct_partner.transaction_type = 'INN'
)
SELECT
    org_uri,
    accountnumber,
    ANY_VALUE(total_transactions) AS hotspot_transactions,
    MIN_BY(source_merchant_site_id, distance_miles) AS closest_site_id,
    MIN(distance_miles) AS closest_site_distance_miles,
    COUNT_IF(distance_miles <= 1) AS sites_within_1_mi,
    COUNT_IF(distance_miles <= 5) AS sites_within_5_mi,
    COUNT_IF(distance_miles <= 10) AS sites_within_10_mi,
    COUNT_IF(distance_miles <= 25) AS sites_within_25_mi
FROM coarse_filter
GROUP BY 1, 2;

-- ============================================================================
-- Step 8: Footprint Classification
-- ============================================================================
CREATE OR REPLACE TEMPORARY TABLE WORKSPACE.digitalda_stage.tmp_account_classification AS
WITH base_parents AS (
    SELECT accountnumber, H3_CELL_TO_PARENT(h3_heatmap_parent_res4, 3) AS res3_cell, H3_CELL_TO_PARENT(h3_heatmap_parent_res4, 2) AS res2_cell, total_transactions 
    FROM WORKSPACE.digitalda_stage.tmp_account_heatmap
),
agg_res4 AS (SELECT accountnumber, MAX(total_transactions) AS max_res4_txns FROM WORKSPACE.digitalda_stage.tmp_account_heatmap GROUP BY 1),
agg_res3 AS (SELECT accountnumber, MAX(txns) AS max_res3_txns FROM (SELECT accountnumber, res3_cell, SUM(total_transactions) AS txns FROM base_parents GROUP BY 1, 2) GROUP BY 1),
agg_res2 AS (SELECT accountnumber, MAX(txns) AS max_res2_txns FROM (SELECT accountnumber, res2_cell, SUM(total_transactions) AS txns FROM base_parents GROUP BY 1, 2) GROUP BY 1),
account_totals AS (SELECT accountnumber, SUM(total_transactions) AS total_txns FROM base_parents GROUP BY 1)
SELECT
    t.accountnumber,
    CASE
        WHEN COALESCE(r4.max_res4_txns, 0) / NULLIF(t.total_txns, 0) >= 0.80 THEN '1 - Metro'
        WHEN COALESCE(r3.max_res3_txns, 0) / NULLIF(t.total_txns, 0) >= 0.80 THEN '2 - Large Metro'
        WHEN COALESCE(r2.max_res2_txns, 0) / NULLIF(t.total_txns, 0) >= 0.80 THEN '3 - State / Regional'
        ELSE '4 - National'
    END AS account_footprint_class
FROM account_totals t
LEFT JOIN agg_res4 r4 ON t.accountnumber = r4.accountnumber
LEFT JOIN agg_res3 r3 ON t.accountnumber = r3.accountnumber
LEFT JOIN agg_res2 r2 ON t.accountnumber = r2.accountnumber;

CREATE OR REPLACE TEMPORARY TABLE WORKSPACE.digitalda_stage.tmp_org_classification AS
WITH org_base_parents AS (
    SELECT ap.org_uri, h.h3_heatmap_parent_res4 AS res4_cell, H3_CELL_TO_PARENT(h.h3_heatmap_parent_res4, 3) AS res3_cell, H3_CELL_TO_PARENT(h.h3_heatmap_parent_res4, 2) AS res2_cell, h.total_transactions
    FROM WORKSPACE.digitalda_stage.tmp_account_heatmap h
    JOIN (SELECT DISTINCT accountnumber, org_uri FROM WORKSPACE.digitalda_stage.tmp_account_points) ap ON h.accountnumber = ap.accountnumber
),
agg_res4 AS (SELECT org_uri, MAX(txns) AS max_res4_txns FROM (SELECT org_uri, h3_heatmap_parent_res4 AS res4_cell, SUM(total_transactions) AS txns FROM org_base_parents GROUP BY 1, 2) GROUP BY 1),
agg_res3 AS (SELECT org_uri, MAX(txns) AS max_res3_txns FROM (SELECT org_uri, res3_cell, SUM(total_transactions) AS txns FROM org_base_parents GROUP BY 1, 2) GROUP BY 1),
agg_res2 AS (SELECT org_uri, MAX(txns) AS max_res2_txns FROM (SELECT org_uri, res2_cell, SUM(total_transactions) AS txns FROM org_base_parents GROUP BY 1, 2) GROUP BY 1),
org_totals AS (SELECT org_uri, SUM(total_transactions) AS total_txns FROM org_base_parents GROUP BY 1)
SELECT
    t.org_uri,
    CASE
        WHEN COALESCE(r4.max_res4_txns, 0) / NULLIF(t.total_txns, 0) >= 0.80 THEN '1 - Metro'
        WHEN COALESCE(r3.max_res3_txns, 0) / NULLIF(t.total_txns, 0) >= 0.80 THEN '2 - Large Metro'
        WHEN COALESCE(r2.max_res2_txns, 0) / NULLIF(t.total_txns, 0) >= 0.80 THEN '3 - State / Regional'
        ELSE '4 - National'
    END AS org_footprint_class
FROM org_totals t
LEFT JOIN agg_res4 r4 ON t.org_uri = r4.org_uri
LEFT JOIN agg_res3 r3 ON t.org_uri = r3.org_uri
LEFT JOIN agg_res2 r2 ON t.org_uri = r2.org_uri;

-- ============================================================================
-- Step 9: Final Output
-- ============================================================================
CREATE OR REPLACE TABLE WORKSPACE.digitalda_stage.ACCOUNT_DISTANCE_DENSITY_C6 AS
SELECT
    m.org_uri,
    m.accountnumber,
    ac.account_footprint_class,
    oc.org_footprint_class,
    m.hotspot_transactions,
    m.closest_site_id,
    m.closest_site_distance_miles,
    m.sites_within_1_mi,
    m.sites_within_5_mi,
    m.sites_within_10_mi,
    m.sites_within_25_mi
FROM WORKSPACE.digitalda_stage.tmp_address_site_metrics m
LEFT JOIN WORKSPACE.digitalda_stage.tmp_account_classification ac ON m.accountnumber = ac.accountnumber
LEFT JOIN WORKSPACE.digitalda_stage.tmp_org_classification oc ON m.org_uri = oc.org_uri;
