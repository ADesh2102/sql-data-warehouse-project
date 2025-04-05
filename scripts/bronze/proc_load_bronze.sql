+--------------------------------------------------------------------------------------------+
| Procedure: bronze.load_bronze                                                              |
|                                                                                            |
| Purpose:                                                                                   |
| This procedure loads data into Bronze Layer tables from CSV files.                         |
| It first truncates each table, then reloads the data using the COPY command.               |
| It also measures and displays how long each table takes to load.                           |
|                                                                                            |
| - Takes no input parameters                                                                |
| - Returns no output                                                                        |
| - Primarily used as part of an ETL pipeline to refresh raw data                          |
|                                                                                            |
| Usage:                                                                                     |
| CALL bronze.load_bronze();                                                                 |
|                                                                                            |
| Notes:                                                                                     |
| Make sure the CSV file paths are accessible to the PostgreSQL server.                      |
+--------------------------------------------------------------------------------------------+

CREATE OR REPLACE Procedure bronze.load_bronze()
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

		start_time := clock_timestamp();
		TRUNCATE TABLE bronze.crm_cust_info;
		COPY bronze.crm_cust_info
		FROM 'D:/PostgreSQL/17/data/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
		DELIMITER ','
		CSV HEADER;
		end_time := clock_timestamp();
		duration := end_time - start_time;
		RAISE NOTICE '>> crm_cust_info loaded in %',duration;

		start_time:= clock_timestamp();
		TRUNCATE TABLE bronze.crm_prd_info;
		COPY bronze.crm_prd_info
		FROM 'D:/PostgreSQL/17/data/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
		DELIMITER ','
		CSV HEADER;
		end_time := clock_timestamp();
		duration := end_time - start_time;
		RAISE NOTICE '>> crm_prd_info loaded in %',duration;
		
		start_time:= clock_timestamp();
		TRUNCATE TABLE bronze.crm_sales_details;
		COPY bronze.crm_sales_details
		FROM 'D:/PostgreSQL/17/data/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
		DELIMITER ','
		CSV HEADER;
		end_time := clock_timestamp();
		duration := end_time - start_time;
		RAISE NOTICE '>> crm_sales_details loaded in %',duration;
	
		RAISE NOTICE '-------------------------------------';
		RAISE NOTICE 'Loading ERP Tables';
		RAISE NOTICE '-------------------------------------';
		
		start_time:= clock_timestamp();
		TRUNCATE TABLE bronze.erp_cust_az12;
		COPY bronze.erp_cust_az12
		FROM 'D:/PostgreSQL/17/data/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
		DELIMITER ','
		CSV HEADER;
		end_time := clock_timestamp();
		duration := end_time - start_time;
		RAISE NOTICE '>> erp_cust_az12 loaded in %',duration;
		
		start_time:= clock_timestamp();
		TRUNCATE TABLE bronze.erp_loc_a101;
		COPY bronze.erp_loc_a101
		FROM 'D:/PostgreSQL/17/data/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
		DELIMITER ','
		CSV HEADER;
		end_time := clock_timestamp();
		duration := end_time - start_time;
		RAISE NOTICE '>> erp_loc_a101 loaded in %',duration;
		
		start_time:= clock_timestamp();
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		COPY bronze.erp_px_cat_g1v2
		FROM 'D:/PostgreSQL/17/data/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
		DELIMITER ','
		CSV HEADER;
		end_time := clock_timestamp();
		duration := end_time - start_time;
		RAISE NOTICE '>> erp_px_cat_g1v2 loaded in %',duration;
		 
		RAISE NOTICE 'Bronze layer loaded successfully!';
		 
	EXCEPTION
		WHEN OTHERS THEN 
			RAISE NOTICE'==========================================';
			RAISE NOTICE 'ERROR OCCURED DURING LOADING BRONZE LAYER';
			RAISE NOTICE 'Error Message: %', SQLERRM;
			RAISE NOTICE'==========================================';
		END;
	
END;
$$;
