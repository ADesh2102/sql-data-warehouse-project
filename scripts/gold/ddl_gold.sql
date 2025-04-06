-- ================================================================
-- Script: GOLD Layer Views DDL
-- Purpose: Create curated, business-ready views in the GOLD layer
--          from cleaned SILVER layer tables.
-- Usage:   Run this script in your data warehouse to refresh
--          dimensional and fact views used in analytics/reporting.
-- Note:    Views are dropped if they already exist before creation.
-- ================================================================


-- =============================================
-- View: gold.dim_customer
-- =============================================
DROP VIEW IF EXISTS gold.dim_customer;

CREATE VIEW gold.dim_customer AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.cntry AS country,
    ci.cst_marital_status AS marital_status,
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'n/a')
    END AS gender,
    ca.bdate AS birthdate,
    ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;


-- =============================================
-- View: gold.dim_products
-- =============================================
DROP VIEW IF EXISTS gold.dim_products;

CREATE VIEW gold.dim_products AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
    pn.prd_id AS product_id,
    pn.prd_key AS product_number,
    pn.prd_nm AS product_name,
    pn.cat_id AS category_id,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pc.maintenance,
    pn.prd_cost AS cost,
    pn.prd_line AS product_line,
    pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL;


-- =============================================
-- View: gold.fact_sales
-- =============================================
DROP VIEW IF EXISTS gold.fact_sales;

CREATE VIEW gold.fact_sales AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY sls_order_dt) AS sales_key,
    sd.sls_ord_num,
    ci.customer_key,
    pi.product_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS ship_date,
    sd.sls_due_dt AS due_date,
    sd.sls_quantity AS quantity,
    sd.sls_price AS price,
    sd.sls_sales AS sales_amount
FROM silver.crm_sales_details sd
JOIN gold.dim_customer ci
    ON sd.sls_cust_id = ci.customer_id
JOIN gold.dim_products pi
    ON sd.sls_prd_key = pi.product_number;
