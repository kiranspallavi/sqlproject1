# ETL Data Pipeline Project

A comprehensive Extract, Transform, Load (ETL) data pipeline built with SQLite for processing customer, product, and sales data with robust auditing and data quality monitoring.

## Project Overview

This ETL pipeline transforms raw CSV data into a structured data warehouse with proper data types, relationships, and comprehensive audit trails. The system includes staging tables for raw data ingestion, production tables with enforced constraints, and automated audit logging for all operations.

## Architecture

### Data Flow
1. **Extract**: Raw CSV files are loaded into staging tables
2. **Transform**: Data is cleaned, validated, and transformed
3. **Load**: Clean data is loaded into production tables with proper relationships
4. **Audit**: All operations are logged with comprehensive audit trails

### Database Schema

#### Staging Tables (Raw Data)
- `stg_customers` - Raw customer data (all TEXT fields)
- `stg_products` - Raw product data (all TEXT fields)
- `stg_sales` - Raw sales transaction data (all TEXT fields)

#### Production Tables (Clean Data)
- `dim_customers` - Customer dimension table
- `dim_products` - Product dimension table
- `fact_sales` - Sales fact table with foreign key relationships

#### Audit Tables
- `audit_etl_log` - Tracks all ETL process executions
- `audit_data_quality` - Records data quality check results
- `audit_record_changes` - Logs all data modifications

## Files Structure

```
├── create_staging_tables.sql      # Creates staging tables for raw data
├── create_production_tables.sql   # Creates production tables with constraints
├── create_audit_tables.sql        # Creates audit and logging tables
├── create_triggers.sql            # Creates automated triggers for auditing
├── data_cleaning.sql              # Data cleaning and validation procedures
├── data_transformation.sql        # Data transformation and loading logic
├── export_procedures.sql          # Creates reporting views and final validation
├── raw_customers.csv              # Sample customer data
├── raw_products.csv               # Sample product data
└── raw_sales.csv                  # Sample sales transaction data
```

## Installation & Setup

### Prerequisites
- SQLite 3.x or higher
- CSV data files in the expected format

### Database Setup
1. Create a new SQLite database:
   ```bash
   sqlite3 etl_database.db
   ```

2. Execute the SQL scripts in order:
   ```sql
   .read create_staging_tables.sql
   .read create_production_tables.sql
   .read create_audit_tables.sql
   .read create_triggers.sql
   ```

### Data Loading
1. Load raw data into staging tables:
   ```sql
   .mode csv
   .import raw_customers.csv stg_customers
   .import raw_products.csv stg_products
   .import raw_sales.csv stg_sales
   ```

2. Execute data processing scripts:
   ```sql
   .read data_cleaning.sql
   .read data_transformation.sql
   .read export_procedures.sql
   ```

## Data Processing Pipeline

### 1. Data Cleaning (`data_cleaning.sql`)
- Removes duplicate records
- Validates email formats
- Cleans phone number formatting
- Removes records with invalid prices/amounts
- Logs all quality checks to audit tables

### 2. Data Transformation (`data_transformation.sql`)
- Converts text fields to appropriate data types
- Calculates derived fields (profit margins, unit prices)
- Establishes foreign key relationships
- Loads clean data into production tables

### 3. Export & Reporting (`export_procedures.sql`)
- Creates comprehensive reporting views
- Generates data quality summaries
- Provides business intelligence views for analysis

## Key Features

### Data Quality Monitoring
- Automated validation of email formats
- Price and quantity validation
- Duplicate detection and removal
- Comprehensive audit logging

### Performance Optimization
- Strategic indexing on frequently queried columns
- Efficient join operations using surrogate keys
- Automated cleanup of old staging data

### Audit Trail
- Complete logging of all ETL operations
- Change tracking for all data modifications
- Data quality check results
- Process execution statistics

## Reporting Views

The system provides several pre-built views for analysis:

- `vw_sales_summary` - Detailed sales transactions with customer and product info
- `vw_etl_process_summary` - ETL process execution statistics
- `vw_data_quality_summary` - Data quality metrics and pass rates
- `vw_sales_performance` - Daily sales performance metrics
- `vw_customer_analysis` - Customer behavior and lifetime value
- `vw_product_performance` - Product sales and profitability analysis
- `vw_sales_rep_performance` - Sales representative performance metrics
- `vw_data_integrity_check` - Data integrity validation results

## Sample Data

The project includes sample data files:
- **Customers**: 10 sample customer records
- **Products**: 10 sample product records across Electronics, Furniture, and Appliances
- **Sales**: 15 sample sales transactions

## Monitoring & Maintenance

### Data Quality Checks
```sql
-- Check data quality summary
SELECT * FROM vw_data_quality_summary;

-- Check for data integrity issues
SELECT * FROM vw_data_integrity_check;
```

### ETL Process Monitoring
```sql
-- View ETL process statistics
SELECT * FROM vw_etl_process_summary;

-- Check recent audit logs
SELECT * FROM audit_etl_log 
ORDER BY start_time DESC 
LIMIT 10;
```

### Performance Monitoring
```sql
-- View sales performance
SELECT * FROM vw_sales_performance;

-- Check customer metrics
SELECT * FROM vw_customer_analysis 
LIMIT 10;
```

## Automated Features

### Triggers
- **Update Timestamps**: Automatically updates `updated_at` fields
- **Audit Logging**: Logs all INSERT/UPDATE operations
- **Data Cleanup**: Removes old staging data (>30 days)
- **Log Maintenance**: Maintains audit log size (last 1000 records)

### Data Validation
- Email format validation
- Price and quantity validation
- Duplicate detection
- Foreign key constraint enforcement

## Business Intelligence

The system provides insights into:
- **Sales Performance**: Daily sales trends and metrics
- **Customer Analysis**: Customer lifetime value and behavior
- **Product Performance**: Best-selling products and profitability
- **Sales Team Performance**: Individual sales representative metrics
- **Data Quality**: Ongoing data quality monitoring and alerts

## Error Handling

- Comprehensive error logging in audit tables
- Failed record tracking with detailed error messages
- Data quality issue identification and reporting
- Process failure notification through audit logs

## Scalability Considerations

- Modular design allows for easy extension
- Configurable data retention policies
- Efficient indexing strategy
- Automated maintenance procedures

## Contributing

When extending this ETL pipeline:
1. Follow the established naming conventions
2. Add appropriate audit logging for new processes
3. Include data quality checks for new data sources
4. Update documentation and create corresponding views
5. Test thoroughly with sample data

## License

This project is provided as-is for educational and development purposes.
