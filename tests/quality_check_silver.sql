/*
================================================================================
Quality Check
================================================================================
Script Purpose: 
  This script performs various quality checks for data consistency, accuracy, and standardization
  across the 'silver' schemas. It includes checks for:
  - Null or duplicates primary keys
  - Unwanted spaces in string fields
  - Data standardization and consistency
  - Invalid date ranges and orders
  - Data consistency between related fields

Usage Notes:
  - Run these tests after loading the silver layer data
*/



-- ================================================================================
-- Checking: 'silver.crm_cust_info' 
-- ================================================================================
-- Check for nulls or duplicates in primary key
-- expectation: no result
SELECT 
prd_Id, 
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL


-- Check for unwanted spaces
-- Expectation: no result
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)


-- Check for NULLs or negative numbers
-- Expectation: no result
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL


-- Data standardization & consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

-- Check for invalid date orders
SELECT * 
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt


-- Date issue fix
SELECT 
prd_id, 
prd_key, 
prd_nm, 
prd_start_dt, 
prd_end_dt, 
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS prd_end_dt_test
FROM silver.crm_prd_info 
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')





-- ================================================================================
-- Checking: 'silver.crm_prd_info' 
-- ================================================================================




-- ================================================================================
-- Checking: 'silver.crm_sales_details' 
-- ================================================================================
-- Check for invalid dates
SELECT 
NULLIF(sls_order_dt, 0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 
OR LENGTH(sls_order_dt::TEXT) != 8
OR sls_order_dt > 20250101
OR sls_order_dt < 19000101


-- Check for correct order date
SELECT 
* 
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt


-- Check data consistency: Between sales, Quantity, and price
-- >> sales = quantity * price
-- >> values must not be NULL, zero, or negative
SELECT DISTINCT
sls_sales AS old_sls_sales, 
sls_quantity, 
sls_price AS old_sls_price,
CASE 
	WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
END AS sls_sales,
CASE 
	WHEN sls_price IS NULL OR sls_price <= 0
		THEN sls_sales / NULLIF(sls_quantity, 0)
	ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price



-- ================================================================================
-- Checking: 'silver.erp_cust_az12' 
-- ================================================================================
-- identify out of range dates
SELECT DISTINCT 
bdate
FROM silver.erp_cust_az12
WHERE bdate < '1923-01-01' OR bdate > NOW()

-- Data standardization & consistency 
SELECT DISTINCT 
gen, 
CASE 
	WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	ELSE 'n/a'
END AS gen
FROM silver.erp_cust_az12

SELECT * FROM silver.erp_cust_az12


-- ================================================================================
-- Checking: 'silver.erp_loc_a101' 
-- ================================================================================
-- Join key check
SELECT cst_key FROM silver.crm_cust_info

-- Data starndardization & consistency
SELECT DISTINCT cntry
FROM silver.erp_loc_a101
ORDER BY cntry

SELECT * FROM silver.erp_loc_a101


-- ================================================================================
-- Checking: 'silver.erp_px_cat_g1v2' 
-- ================================================================================
-- Check for unwanted spaces
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE TRIM(cat) != cat OR TRIM(subcat) != subcat OR TRIM(maintenance) != maintenance

-- Check standardization
SELECT DISTINCT 
maintenance
FROM bronze.erp_px_cat_g1v2

