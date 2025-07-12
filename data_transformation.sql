-- Data transformation and loading to production tables

-- Log transformation start
INSERT INTO audit_etl_log (process_name, table_name, operation_type, start_time, status)
VALUES ('DATA_TRANSFORMATION', 'PRODUCTION_TABLES', 'TRANSFORM_LOAD', datetime('now'), 'RUNNING');

-- Transform and load customers dimension
INSERT INTO dim_customers (
    customer_id, first_name, last_name, email, phone, 
    address, city, state, zip_code, registration_date
)
SELECT 
    CAST(customer_id AS INTEGER),
    TRIM(first_name),
    TRIM(last_name),
    LOWER(TRIM(email)),
    phone,
    TRIM(address),
    TRIM(city),
    UPPER(TRIM(state)),
    TRIM(zip_code),
    DATE(registration_date)
FROM stg_customers
WHERE customer_id NOT IN (SELECT customer_id FROM dim_customers);

-- Log customer dimension load
INSERT INTO audit_etl_log (process_name, table_name, operation_type, start_time, end_time, status, records_affected)
VALUES ('DATA_TRANSFORMATION', 'dim_customers', 'INSERT', datetime('now'), datetime('now'), 'SUCCESS', 
        (SELECT COUNT(*) FROM dim_customers));

-- Transform and load products dimension
INSERT INTO dim_products (
    product_id, product_name, category, price, cost, 
    supplier, stock_quantity, profit_margin, created_date
)
SELECT 
    CAST(product_id AS INTEGER),
    TRIM(product_name),
    TRIM(category),
    CAST(price AS DECIMAL(10,2)),
    CAST(cost AS DECIMAL(10,2)),
    TRIM(supplier),
    CAST(stock_quantity AS INTEGER),
    ROUND(((CAST(price AS REAL) - CAST(cost AS REAL)) / CAST(price AS REAL)) * 100, 2),
    DATE(created_date)
FROM stg_products
WHERE product_id NOT IN (SELECT product_id FROM dim_products);

-- Log product dimension load
INSERT INTO audit_etl_log (process_name, table_name, operation_type, start_time, end_time, status, records_affected)
VALUES ('DATA_TRANSFORMATION', 'dim_products', 'INSERT', datetime('now'), datetime('now'), 'SUCCESS', 
        (SELECT COUNT(*) FROM dim_products));

-- Transform and load sales fact table
INSERT INTO fact_sales (
    transaction_id, customer_key, product_key, quantity, 
    sale_date, unit_price, total_amount, discount_amount, 
    net_amount, sales_rep
)
SELECT 
    CAST(s.transaction_id AS INTEGER),
    c.customer_key,
    p.product_key,
    CAST(s.quantity AS INTEGER),
    DATE(s.sale_date),
    CAST(s.sale_amount AS DECIMAL(10,2)) / CAST(s.quantity AS INTEGER),
    CAST(s.sale_amount AS DECIMAL(10,2)),
    CAST(s.discount_amount AS DECIMAL(10,2)),
    CAST(s.sale_amount AS DECIMAL(10,2)) - CAST(s.discount_amount AS DECIMAL(10,2)),
    TRIM(s.sales_rep)
FROM stg_sales s
JOIN dim_customers c ON CAST(s.customer_id AS INTEGER) = c.customer_id
JOIN dim_products p ON CAST(s.product_id AS INTEGER) = p.product_id
WHERE s.transaction_id NOT IN (SELECT transaction_id FROM fact_sales);

-- Log sales fact load
INSERT INTO audit_etl_log (process_name, table_name, operation_type, start_time, end_time, status, records_affected)
VALUES ('DATA_TRANSFORMATION', 'fact_sales', 'INSERT', datetime('now'), datetime('now'), 'SUCCESS', 
        (SELECT COUNT(*) FROM fact_sales));

-- Update main transformation log
UPDATE audit_etl_log 
SET end_time = datetime('now'), 
    status = 'SUCCESS',
    records_affected = (
        SELECT COUNT(*) FROM dim_customers
    ) + (
        SELECT COUNT(*) FROM dim_products
    ) + (
        SELECT COUNT(*) FROM fact_sales
    )
WHERE process_name = 'DATA_TRANSFORMATION' 
  AND table_name = 'PRODUCTION_TABLES' 
  AND status = 'RUNNING';

-- Create summary statistics
CREATE VIEW IF NOT EXISTS vw_sales_summary AS
SELECT 
    c.first_name || ' ' || c.last_name as customer_name,
    p.product_name,
    p.category,
    f.quantity,
    f.unit_price,
    f.total_amount,
    f.discount_amount,
    f.net_amount,
    f.sale_date,
    f.sales_rep
FROM fact_sales f
JOIN dim_customers c ON f.customer_key = c.customer_key
JOIN dim_products p ON f.product_key = p.product_key
ORDER BY f.sale_date DESC;