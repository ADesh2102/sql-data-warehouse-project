+------------------------------------------------------------------------------------------------------------+
| SCRIPT PURPOSE: Loads cleaned and standardized data from Bronze Layer into Silver Layer tables.            |
|                                                                                                            |
| ACTIONS PERFORMED: Truncates silver tables, transforms data (e.g., trimming, formatting, deduplication),   |
| calculates durations, logs load times, and handles errors using exception blocks.                          |
|                                                                                                            |
| PARAMETERS & RETURNS: Takes no parameters. Returns nothing. Updates multiple Silver Layer tables.          |
|                                                                                                            |
| USAGE EXAMPLE: CALL silver.load_silver();                                                                  |
+------------------------------------------------------------------------------------------------------------+


CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE 
	start_time TIMESTAMP;
	end_time TIMESTAMP;
	duration INTERVAL;
BEGIN
	
	RAISE NOTICE '=====================================';
	RAISE NOTICE 'Loading Bronze Layer';
	RAISE NOTICE '=====================================';
	
	BEGIN 
		RAISE NOTICE '-------------------------------------';
		RAISE NOTICE 'Loading CRM Tables';
		RAISE NOTICE '-------------------------------------';
			
		BEGIN
			start_time := clock_timestamp();
			RAISE NOTICE '>> Truncating Table: silver.crm_cust_info';
			TRUNCATE TABLE silver.crm_cust_info;
			RAISE NOTICE '>> Inserting Data Into: silver.crm_cust_info';
			INSERT INTO silver.crm_cust_info(
				cst_id,
				cst_key,
				cst_firstname,
				cst_lastname,
				cst_marital_status,
				cst_gndr,
				cst_create_date
				)
			SELECT 
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				ELSE 'n/a'
			END cst_marital_status,
			CASE
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'n/a'
			END cst_gndr,
			
			cst_create_date
			FROM (
			SELECT * ,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
			FROM bronze.crm_cust_info)
			WHERE flag_last = 1 ;
			end_time := clock_timestamp();
			duration := end_time - start_time;
			RAISE NOTICE '>> crm_cust_info loaded in %',duration;

			start_time := clock_timestamp();
			RAISE NOTICE '>> Truncating Table: silver.crm_prd_info';
			TRUNCATE TABLE silver.crm_prd_info;
			RAISE NOTICE '>> Inserting Data Into: silver.crm_prd_info';
			INSERT INTO silver.crm_prd_info(
				prd_id,
				cat_id,
				prd_key,
				prd_nm,
				prd_cost,
				prd_line,
				prd_start_dt,
				prd_end_dt
			)
			SELECT 
			prd_id,
			REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
			SUBSTRING(prd_key,7, LENGTH(prd_key)) AS prd_key,
			prd_nm,
			COALESCE(prd_cost, 0) AS prd_cost,
			CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
				 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
				 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
				 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
				 ELSE 'n/a'
			END prd_line,
			prd_start_dt::DATE AS prd_start_dt,
			(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER by prd_start_dt)- INTERVAL '1 day')::DATE as prd_end_dt
			FROM bronze.crm_prd_info;
			end_time := clock_timestamp();
			duration := end_time - start_time;
			RAISE NOTICE '>> crm_prd_info loaded in %',duration;

			start_time := clock_timestamp();
			RAISE NOTICE '>> Truncating Table: silver.crm_sales_details';
			TRUNCATE TABLE silver.crm_sales_details;
			RAISE NOTICE '>> Inserting Data Into: silver.crm_sales_details';
			INSERT INTO silver.crm_sales_details(
				sls_ord_num,
				sls_prd_key,
				sls_cust_id,
				sls_order_dt,
				sls_ship_dt,
				sls_due_dt,
				sls_sales,
				sls_quantity,
				sls_price
			)
			SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE WHEN sls_order_dt <= 0 or LENGTH(CAST(sls_order_dt AS TEXT)) != 8 THEN NULL
				ELSE CAST(TO_DATE(CAST(sls_order_dt AS TEXT),'YYYYMMDD') AS DATE) 
			END AS sls_order_dt,
			
			CASE WHEN sls_ship_dt <= 0 OR LENGTH(CAST(sls_ship_dt AS TEXT)) != 8 THEN NULL
				ELSE CAST(TO_DATE(CAST(sls_ship_dt AS TEXT), 'YYYYMMDD') AS DATE)
			END AS sls_ship_dt,
			
			CASE WHEN sls_due_dt <= 0 OR LENGTH(CAST(sls_due_dt AS TEXT)) != 8 THEN NULL
				ELSE CAST(TO_DATE(CAST(sls_due_dt AS TEXT), 'YYYYMMDD') AS DATE)
			END AS sls_due_dt,
			CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
					THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales 
			END AS sls_sales,
			sls_quantity,
			CASE WHEN sls_price IS NULL or sls_price <= 0 
					THEN sls_sales / NULLIF(sls_quantity,0)
				ELSE sls_price
			END AS sls_price
			
			FROM bronze.crm_sales_details;
			end_time := clock_timestamp();
			duration := end_time - start_time;
			RAISE NOTICE '>> crm_sales_details loaded in %',duration;

		RAISE NOTICE '-------------------------------------';
		RAISE NOTICE 'Loading ERP Tables';
		RAISE NOTICE '-------------------------------------';
			

			start_time := clock_timestamp();
			RAISE NOTICE '>> Truncating Table: silver.erp_cust_az12';
			TRUNCATE TABLE silver.erp_cust_az12;
			RAISE NOTICE '>> Inserting Data Into: silver.erp_cust_az12';
			INSERT INTO silver.erp_cust_az12(
				cid,
				bdate,
				gen
			)
			SELECT 
			CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LENGTH(cid))
				ELSE cid
			END AS cid,
			CASE WHEN bdate > CURRENT_DATE THEN NULL
				ELSE bdate
			END AS bdate,
			
			CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
				when UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
				ELSE 'n/a'
			END AS gen
			FROM bronze.erp_cust_az12;
			end_time := clock_timestamp();
			duration := end_time - start_time;
			RAISE NOTICE '>> erp_cust_az12 loaded in %',duration;

			start_time := clock_timestamp();
			RAISE NOTICE '>> Truncating Table: silver.erp_loc_a101';
			TRUNCATE TABLE silver.erp_loc_a101;
			RAISE NOTICE '>> Inserting Data Into: silver.erp_loc_a101';
			INSERT INTO silver.erp_loc_a101(cid,cntry)
			SELECT 
			REPLACE(cid, '-','') cid,
			CASE WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
				WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				WHEN TRIM(cntry) = '' OR TRIM(cntry) IS NULL THEN 'n/a'
				ELSE TRIM(cntry)
			END AS cntry
			FROM bronze.erp_loc_a101;
			end_time := clock_timestamp();
			duration := end_time - start_time;
			RAISE NOTICE '>> bronze.erp_loc_a101 loaded in %',duration;
		
			start_time := clock_timestamp();
			RAISE NOTICE '>> Truncating Table: silver.erp_px_cat_g1v2';
			TRUNCATE TABLE silver.erp_px_cat_g1v2;
			RAISE NOTICE '>> Inserting Data Into: silver.erp_px_cat_g1v2';
			INSERT INTO silver.erp_px_cat_g1v2(
			id,
			cat,
			subcat,
			maintenance
			)
			SELECT 
			id,
			cat,
			subcat,
			maintenance
			FROM bronze.erp_px_cat_g1v2;
			end_time := clock_timestamp();
			duration := end_time - start_time;
			RAISE NOTICE '>> bronze.erp_px_cat_g1v2 loaded in %',duration;
			RAISE NOTICE 'SILVER layer loaded successfully!';
		EXCEPTION
		WHEN OTHERS THEN 
			RAISE NOTICE'==========================================';
			RAISE NOTICE 'ERROR OCCURED DURING LOADING SILVER LAYER';
			RAISE NOTICE 'Error Message: %', SQLERRM;
			RAISE NOTICE'==========================================';
		END;
	END;
END;
$$;
