-- ===============================================================================
-- Stored Procedure: Load Bronze Layer (Source -> Bronze)
-- ===============================================================================
-- Script Purpose:
--     This procedure loads data into the 'bronze' schema from external CSV files.
--     Steps:
--       - Truncate bronze tables before loading.
--       - Use COPY (Postgres equivalent of BULK INSERT) to load CSV files.
--       - Delimiter = ',' and CSV HEADER ensures the first row is ignored.
--
-- Usage Example:
--     CALL bronze.load_bronze();
-- ===============================================================================

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time   TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time   TIMESTAMP;
BEGIN
	BEGIN
	    batch_start_time := clock_timestamp();
	    RAISE NOTICE '================================================';
	    RAISE NOTICE 'Loading Bronze Layer';
	    RAISE NOTICE '================================================';
	
	    -- ================= CRM ==================
	    RAISE NOTICE '------------------------------------------------';
	    RAISE NOTICE 'Loading CRM Tables';
	    RAISE NOTICE '------------------------------------------------';
	
	    -- bronze.crm_cust_info
	    start_time := clock_timestamp();
	    TRUNCATE TABLE bronze.crm_cust_info;
	    COPY bronze.crm_cust_info
	    FROM '/Users/eugene/Desktop/DE/data_warehoousing_project/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
	    DELIMITER ','
	    CSV HEADER;
	    end_time := clock_timestamp();
	    RAISE NOTICE '>> bronze.crm_cust_info loaded in % seconds', EXTRACT(epoch FROM end_time - start_time);
	
	    -- bronze.crm_prd_info
	    start_time := clock_timestamp();
	    TRUNCATE TABLE bronze.crm_prd_info;
	    COPY bronze.crm_prd_info
	    FROM '/Users/eugene/Desktop/DE/data_warehoousing_project/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
	    DELIMITER ','
	    CSV HEADER;
	    end_time := clock_timestamp();
	    RAISE NOTICE '>> bronze.crm_prd_info loaded in % seconds', EXTRACT(epoch FROM end_time - start_time);
	
	    -- bronze.crm_sales_details
	    start_time := clock_timestamp();
	    TRUNCATE TABLE bronze.crm_sales_details;
	    COPY bronze.crm_sales_details
	    FROM '/Users/eugene/Desktop/DE/data_warehoousing_project/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
	    DELIMITER ','
	    CSV HEADER;
	    end_time := clock_timestamp();
	    RAISE NOTICE '>> bronze.crm_sales_details loaded in % seconds', EXTRACT(epoch FROM end_time - start_time);
	
	    -- ================= ERP ==================
	    RAISE NOTICE '------------------------------------------------';
	    RAISE NOTICE 'Loading ERP Tables';
	    RAISE NOTICE '------------------------------------------------';
	
	    -- bronze.erp_cust_az12
	    start_time := clock_timestamp();
	    TRUNCATE TABLE bronze.erp_cust_az12;
	    COPY bronze.erp_cust_az12
	    FROM '/Users/eugene/Desktop/DE/data_warehoousing_project/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
	    DELIMITER ','
	    CSV HEADER;
	    end_time := clock_timestamp();
	    RAISE NOTICE '>> bronze.erp_cust_az12 loaded in % seconds', EXTRACT(epoch FROM end_time - start_time);
	
	    -- bronze.erp_loc_a101
	    start_time := clock_timestamp();
	    TRUNCATE TABLE bronze.erp_loc_a101;
	    COPY bronze.erp_loc_a101
	    FROM '/Users/eugene/Desktop/DE/data_warehoousing_project/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
	    DELIMITER ','
	    CSV HEADER;
	    end_time := clock_timestamp();
	    RAISE NOTICE '>> bronze.erp_loc_a101 loaded in % seconds', EXTRACT(epoch FROM end_time - start_time);
	
	    -- bronze.erp_px_cat_g1v2
	    start_time := clock_timestamp();
	    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	    COPY bronze.erp_px_cat_g1v2
	    FROM '/Users/eugene/Desktop/DE/data_warehoousing_project/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
	    DELIMITER ','
	    CSV HEADER;
	    end_time := clock_timestamp();
	    RAISE NOTICE '>> bronze.erp_px_cat_g1v2 loaded in % seconds', EXTRACT(epoch FROM end_time - start_time);
	
	    -- ================= End ==================
	    batch_end_time := clock_timestamp();
	    RAISE NOTICE '==========================================';
	    RAISE NOTICE 'Loading Bronze Layer is Completed';
	    RAISE NOTICE '   - Total Load Duration: % seconds', EXTRACT(epoch FROM batch_end_time - batch_start_time);
	    RAISE NOTICE '==========================================';

	EXCEPTION
	    WHEN OTHERS THEN
	        RAISE NOTICE '==========================================';
	        RAISE NOTICE 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
	        RAISE NOTICE 'Error Message: %', SQLERRM;
	        RAISE NOTICE '==========================================';
	END;
END;
$$;
