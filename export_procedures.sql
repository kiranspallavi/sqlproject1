-- Export procedures and final data validation

-- Create comprehensive reporting views
CREATE VIEW IF NOT EXISTS vw_etl_process_summary AS
SELECT 
    process_name,
    table_name,
    operation_type,
    COUNT(*) as execution_count,
    SUM(records_affected) as total_records_processed,
    AVG(JULIANDAY(end_time) - JULIANDAY(start_time)) * 24 * 60 as avg_duration_minutes,
    MIN(start_time) as first_execution,
    MAX(end_time) as last_execution,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_runs,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_runs
FROM audit_etl_log
WHERE end_time IS NOT NULL
GROUP BY process_name, table_name, operation_type
ORDER BY last_execution DESC;

-- Create data quality summary view
CREATE VIEW IF NOT EXISTS vw_data_quality_summary AS
SELECT 
    table_name,
    quality_check,
    COUNT(*) as total_checks,
    SUM(CASE WHEN check_result = 'PASS' THEN 1 ELSE 0 END) as passed_checks,
    SUM(CASE WHEN check_result = 'FAIL' THEN 1 ELSE 0 END) as failed_checks,
    SUM(CASE WHEN check_result = 'WARNING' THEN 1 ELSE 0 END) as warning_checks,
    ROUND(
        (SUM(CASE WHEN check_result = 'PASS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2
    ) as pass_rate_percent,
    MAX(check_timestamp) as last_check_date
FROM audit_data_quality
GROUP BY table_name, quality_check
ORDER BY table_name, quality_check;

-- Create sales performance view
CREATE VIEW IF NOT EXISTS vw_sales_performance AS
SELECT 
    DATE(f.sale_date) as sale_date,
    COUNT(*) as total_transactions,
    SUM(f.quantity) as total_items_sold,
    SUM(f.total_amount) as gross_sales,
    SUM(f.discount_amount) as total_discounts,
    SUM(f.net_amount) as net_sales,
    AVG(f.total_amount) as avg_transaction_value,
    COUNT(DISTINCT f.customer_key) as unique_customers
FROM fact_sales f
GROUP BY DATE(f.sale_date)
ORDER BY sale_date DESC;

-- Create customer analysis view
CREATE VIEW IF NOT EXISTS vw_customer_analysis AS
SELECT 
    c.customer_key,
    c.first_name || ' ' || c.last_name as customer_name,
    c.email,
    c.city,
    c.state,
    COUNT(f.transaction_id) as total_orders,
    SUM(f.quantity) as total_items_purchased,
    SUM(f.total_amount) as total_spent,
    AVG(f.total_amount) as avg_order_value,
    MIN(f.sale_date) as first_purchase_date,
    MAX(f.sale_date) as last_purchase_date,
    JULIANDAY(MAX(f.sale_date)) - JULIANDAY(MIN(f.sale_date)) as customer_lifetime_days
FROM dim_customers c
LEFT JOIN fact_sales f ON c.customer_key = f.customer_key
GROUP BY c.customer_key, c.first_name, c.last_name, c.email, c.city, c.state
ORDER BY total_spent DESC;

-- Create product performance view
CREATE VIEW IF NOT EXISTS vw_product_performance AS
SELECT 
    p.product_key,
    p.product_name,
    p.category,
    p.price,
    p.cost,
    p.profit_margin,
    p.supplier,
    COUNT(f.transaction_id) as times_sold,
    SUM(f.quantity) as total_quantity_sold,
    SUM(f.total_amount) as total_revenue,
    SUM(f.net_amount) as total_net_revenue,
    SUM(f.quantity * p.cost) as total_cost,
    SUM(f.net_amount) - SUM(f.quantity * p.cost) as total_profit,
    AVG(f.total_amount) as avg_sale_price
FROM dim_products p
LEFT JOIN fact_sales f ON p.product_key = f.product_key
GROUP BY p.product_key, p.product_name, p.category, p.price, p.cost, p.profit_margin, p.supplier
ORDER BY total_revenue DESC;

-- Create sales rep performance view
CREATE VIEW IF NOT EXISTS vw_sales_rep_performance AS
SELECT 
    f.sales_rep,
    COUNT(*) as total_sales,
    SUM(f.quantity) as total_items_sold,
    SUM(f.total_amount) as gross_sales,
    SUM(f.net_amount) as net_sales,
    AVG(f.total_amount) as avg_sale_amount,
    COUNT(DISTINCT f.customer_key) as unique_customers_served,
    COUNT(DISTINCT f.product_key) as unique_products_sold,
    MIN(f.sale_date) as first_sale_date,
    MAX(f.sale_date) as last_sale_date
FROM fact_sales f
WHERE f.sales_rep IS NOT NULL
GROUP BY f.sales_rep
ORDER BY net_sales DESC;

-- Final validation queries
-- Check for data integrity issues
CREATE VIEW IF NOT EXISTS vw_data_integrity_check AS
SELECT 
    'Orphaned Sales Records' as issue_type,
    COUNT(*) as issue_count,
    'Sales records without matching customer or product' as description
FROM fact_sales f
LEFT JOIN dim_customers c ON f.customer_key = c.customer_key
LEFT JOIN dim_products p ON f.product_key = p.product_key
WHERE c.customer_key IS NULL OR p.product_key IS NULL

UNION ALL

SELECT 
    'Negative Sales Amounts' as issue_type,
    COUNT(*) as issue_count,
    'Sales records with negative amounts' as description
FROM fact_sales
WHERE total_amount < 0 OR net_amount < 0

UNION ALL

SELECT 
    'Invalid Email Addresses' as issue_type,
    COUNT(*) as issue_count,
    'Customer records with invalid email format' as description
FROM dim_customers
WHERE email NOT LIKE '%@%.%'

UNION ALL

SELECT 
    'Products with Zero Price' as issue_type,
    COUNT(*) as issue_count,
    'Product records with zero or negative price' as description
FROM dim_products
WHERE price <= 0

ORDER BY issue_count DESC;