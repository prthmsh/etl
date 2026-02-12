-- Database setup for ETL Pipeline
-- Execute this script to create the database and table

-- Create database
CREATE DATABASE IF NOT EXISTS etl_db
CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;

-- Use the database
USE etl_db;

-- Create the main ETL data table
CREATE TABLE IF NOT EXISTS etl_data (
    id VARCHAR(50) PRIMARY KEY,
    transaction_date DATETIME,
    customer_name VARCHAR(255),
    amount DECIMAL(10,2),
    status VARCHAR(50),
    created_at DATETIME,
    etl_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_transaction_date (transaction_date),
    INDEX idx_customer_name (customer_name),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Create ETL user with appropriate permissions
-- Replace 'your_password' with a strong password
CREATE USER IF NOT EXISTS 'etl_user'@'localhost' IDENTIFIED BY 'your_password';

-- Grant permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON etl_db.* TO 'etl_user'@'localhost';
FLUSH PRIVILEGES;

-- Optional: Create audit log table
CREATE TABLE IF NOT EXISTS etl_audit_log (
    log_id INT AUTO_INCREMENT PRIMARY KEY,
    pipeline_run_id VARCHAR(50),
    server_name VARCHAR(100),
    rows_extracted INT,
    rows_loaded INT,
    status VARCHAR(50),
    error_message TEXT,
    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_run_id (pipeline_run_id),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Display table structure
DESCRIBE etl_data;
DESCRIBE etl_audit_log;
