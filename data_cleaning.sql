-- Data cleaning and validation procedures

-- Function to log ETL operations
INSERT INTO audit_etl_log (process_name, table_name, operation_type, start_time, status)
VALUES ('DATA_CLEANING', 'ALL_STAGING_TABLES', 'CLEAN', datetime('now'), 'RUNNING');

-- Clean customers data
-- Remove duplicates and invalid records
DELETE FROM stg_customers WHERE customer_id IN (
    SELECT customer_id FROM stg_customers 
    GROUP BY customer_id HAVING COUNT(*) > 1 
    AND rowid NOT IN (
        SELECT MIN(rowid) FROM stg_customers 
        GROUP BY customer_id
    )
);

-- Remove customers with invalid email format
DELETE FROM stg_customers 
WHERE email NOT LIKE '%@%.%' OR email IS NULL OR email = '';

-- Clean phone numbers (remove non-numeric characters except dashes)
UPDATE stg_customers 
SET phone = CASE 
    WHEN phone = '' THEN NULL 
    ELSE phone 
END;

-- Clean products data
-- Remove duplicates
DELETE FROM stg_products WHERE product_id IN (
    SELECT product_id FROM stg_products 
    GROUP BY product_id HAVING COUNT(*) > 1 
    AND rowid NOT IN (
        SELECT MIN(rowid) FROM stg_products 
        GROUP BY product_id
    )
);

-- Remove products with invalid prices
DELETE FROM stg_products 
WHERE CAST(price AS REAL) <= 0 OR price IS NULL OR price = '';

-- Clean sales data
-- Remove duplicates
DELETE FROM stg_sales WHERE transaction_id IN (
    SELECT transaction_id FROM stg_sales 
    GROUP BY transaction_id HAVING COUNT(*) > 1 
    AND rowid NOT IN (
        SELECT MIN(rowid) FROM stg_sales 
        GROUP BY transaction_id
    )
);

-- Remove sales with invalid amounts or quantities
DELETE FROM stg_sales 
WHERE CAST(sale_amount AS REAL) <= 0 
   OR CAST(quantity AS INTEGER) <= 0
   OR sale_amount IS NULL 
   OR quantity IS NULL;

-- Log data quality checks
INSERT INTO audit_data_quality (table_name, quality_check, check_result, records_checked, details)
SELECT 
    'stg_customers' as table_name,
    'email_format_check' as quality_check,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as check_result,
    (SELECT COUNT(*) FROM stg_customers) as records_checked,
    'Invalid email formats: ' || COUNT(*) as details
FROM stg_customers 
WHERE email NOT LIKE '%@%.%' OR email IS NULL OR email = '';

INSERT INTO audit_data_quality (table_name, quality_check, check_result, records_checked, details)
SELECT 
    'stg_products' as table_name,
    'price_validation' as quality_check,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as check_result,
    (SELECT COUNT(*) FROM stg_products) as records_checked,
    'Products with invalid prices: ' || COUNT(*) as details
FROM stg_products 
WHERE CAST(price AS REAL) <= 0 OR price IS NULL OR price = '';

-- Update ETL log
UPDATE audit_etl_log 
SET end_time = datetime('now'), 
    status = 'SUCCESS',
    records_affected = (
        SELECT COUNT(*) FROM stg_customers
    ) + (
        SELECT COUNT(*) FROM stg_products
    ) + (
        SELECT COUNT(*) FROM stg_sales
    )
WHERE process_name = 'DATA_CLEANING' 
  AND table_name = 'ALL_STAGING_TABLES' 
  AND status = 'RUNNING';