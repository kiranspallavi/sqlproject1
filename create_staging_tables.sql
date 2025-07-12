-- Create staging tables for raw data import
-- These tables will accept all data types as TEXT initially

CREATE TABLE IF NOT EXISTS stg_customers (
    customer_id TEXT,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    phone TEXT,
    address TEXT,
    city TEXT,
    state TEXT,
    zip_code TEXT,
    registration_date TEXT,
    load_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg_products (
    product_id TEXT,
    product_name TEXT,
    category TEXT,
    price TEXT,
    cost TEXT,
    supplier TEXT,
    stock_quantity TEXT,
    created_date TEXT,
    load_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg_sales (
    transaction_id TEXT,
    customer_id TEXT,
    product_id TEXT,
    quantity TEXT,
    sale_date TEXT,
    sale_amount TEXT,
    discount_amount TEXT,
    sales_rep TEXT,
    load_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_stg_customers_id ON stg_customers(customer_id);
CREATE INDEX IF NOT EXISTS idx_stg_products_id ON stg_products(product_id);
CREATE INDEX IF NOT EXISTS idx_stg_sales_transaction ON stg_sales(transaction_id);