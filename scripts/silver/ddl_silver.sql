+-------------------------------------------------------------------------------------------+
| This script drops existing tables (if any) and creates new Silver Layer tables            |
| for the data warehouse. These tables contain cleaned, standardized, and enriched          |
| data derived from raw CRM and ERP Bronze Layer sources.                                   |
|                                                                                           |
| The Silver Layer ensures data quality by applying business logic, handling nulls,         |
| removing duplicates, and formatting fields. These curated tables serve as the trusted     |
| intermediate source for analytics, dashboards, and further processing into the Gold Layer.|
+-------------------------------------------------------------------------------------------+
DROP TABLE IF EXISTS silver.crm_cust_info;
DROP TABLE IF EXISTS silver.crm_prd_info;
DROP TABLE IF EXISTS silver.crm_sales_details;
DROP TABLE IF EXISTS silver.erp_cust_az12;
DROP TABLE IF EXISTS silver.erp_loc_a101;
DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;

CREATE TABLE  silver.crm_cust_info(
	cst_id INT,
	cst_key VARCHAR(50),
	cst_firstname VARCHAR(50) ,
	cst_lastname VARCHAR(50),
	cst_marital_status VARCHAR(50),
	cst_gndr VARCHAR(50),
	cst_create_date DATE,
	dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info(
	prd_id INT,
	cat_id VARCHAR(50),
	prd_key VARCHAR(50),
	prd_nm  VARCHAR(100),
	prd_cost INT,
	prd_line VARCHAR(50),
	prd_start_dt Date,
	prd_end_dt DATE,
	dwh_create_date DATE DEFAULT CURRENT_DATE
);

DROP TABLE IF EXISTS silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details(
	sls_ord_num VARCHAR(50),
	sls_prd_key VARCHAR(50),
	sls_cust_id INT,
	sls_order_dt DATE,
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE silver.erp_cust_az12(
	CID VARCHAR(50),
	BDATE DATE,
	GEN VARCHAR(50),
	dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE silver.erp_loc_a101(
	CID VARCHAR(50),
	CNTRY VARCHAR(50),
	dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE silver.erp_px_cat_g1v2(
	ID VARCHAR(50),
	CAT VARCHAR(50),
	SUBCAT VARCHAR(50),
	MAINTENANCE VARCHAR(50),
	dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
