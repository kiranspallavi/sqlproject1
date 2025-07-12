-- Create triggers for automatic cleanup and audit tracking

-- Trigger to update the updated_at timestamp in dimension tables
CREATE TRIGGER IF NOT EXISTS trg_customers_updated_at
    AFTER UPDATE ON dim_customers
    FOR EACH ROW
BEGIN
    UPDATE dim_customers 
    SET updated_at = CURRENT_TIMESTAMP 
    WHERE customer_key = NEW.customer_key;
END;

CREATE TRIGGER IF NOT EXISTS trg_products_updated_at
    AFTER UPDATE ON dim_products
    FOR EACH ROW
BEGIN
    UPDATE dim_products 
    SET updated_at = CURRENT_TIMESTAMP 
    WHERE product_key = NEW.product_key;
END;

-- Trigger to log all changes to production tables
CREATE TRIGGER IF NOT EXISTS trg_audit_customers_insert
    AFTER INSERT ON dim_customers
    FOR EACH ROW
BEGIN
    INSERT INTO audit_record_changes (table_name, record_id, operation, new_values)
    VALUES ('dim_customers', NEW.customer_key, 'INSERT', 
            'customer_id: ' || NEW.customer_id || ', name: ' || NEW.first_name || ' ' || NEW.last_name);
END;

CREATE TRIGGER IF NOT EXISTS trg_audit_customers_update
    AFTER UPDATE ON dim_customers
    FOR EACH ROW
BEGIN
    INSERT INTO audit_record_changes (table_name, record_id, operation, old_values, new_values)
    VALUES ('dim_customers', NEW.customer_key, 'UPDATE', 
            'customer_id: ' || OLD.customer_id || ', name: ' || OLD.first_name || ' ' || OLD.last_name,
            'customer_id: ' || NEW.customer_id || ', name: ' || NEW.first_name || ' ' || NEW.last_name);
END;

CREATE TRIGGER IF NOT EXISTS trg_audit_products_insert
    AFTER INSERT ON dim_products
    FOR EACH ROW
BEGIN
    INSERT INTO audit_record_changes (table_name, record_id, operation, new_values)
    VALUES ('dim_products', NEW.product_key, 'INSERT', 
            'product_id: ' || NEW.product_id || ', name: ' || NEW.product_name || ', price: ' || NEW.price);
END;

CREATE TRIGGER IF NOT EXISTS trg_audit_sales_insert
    AFTER INSERT ON fact_sales
    FOR EACH ROW
BEGIN
    INSERT INTO audit_record_changes (table_name, record_id, operation, new_values)
    VALUES ('fact_sales', NEW.sales_key, 'INSERT', 
            'transaction_id: ' || NEW.transaction_id || ', amount: ' || NEW.total_amount);
END;

-- Trigger for automatic cleanup of old staging data (older than 30 days)
CREATE TRIGGER IF NOT EXISTS trg_cleanup_old_staging
    AFTER INSERT ON audit_etl_log
    FOR EACH ROW
    WHEN NEW.process_name = 'DATA_TRANSFORMATION' AND NEW.status = 'SUCCESS'
BEGIN
    DELETE FROM stg_customers WHERE load_timestamp < datetime('now', '-30 days');
    DELETE FROM stg_products WHERE load_timestamp < datetime('now', '-30 days');
    DELETE FROM stg_sales WHERE load_timestamp < datetime('now', '-30 days');
END;

-- Trigger to maintain audit log size (keep only last 1000 records)
CREATE TRIGGER IF NOT EXISTS trg_cleanup_audit_log
    AFTER INSERT ON audit_etl_log
    FOR EACH ROW
    WHEN (SELECT COUNT(*) FROM audit_etl_log) > 1000
BEGIN
    DELETE FROM audit_etl_log 
    WHERE log_id IN (
        SELECT log_id FROM audit_etl_log 
        ORDER BY log_id ASC 
        LIMIT (SELECT COUNT(*) FROM audit_etl_log) - 1000
    );
END;