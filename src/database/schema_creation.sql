DROP DATABASE IF EXISTS creditflow360;
CREATE DATABASE creditflow360 
CHARACTER SET utf8mb4 
COLLATE utf8mb4_unicode_ci;

USE creditflow360;


CREATE TABLE dim_customer (
    customer_sk INT AUTO_INCREMENT PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL UNIQUE,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    date_of_birth DATE NOT NULL,
    age INT,
    gender VARCHAR(10),
    marital_status VARCHAR(20),
    education VARCHAR(50),
    employment_type VARCHAR(50),
    annual_income DECIMAL(15,2) DEFAULT 0,
    income_tier VARCHAR(20),
    credit_score INT,
    credit_tier VARCHAR(20),
    city VARCHAR(100),
    state VARCHAR(50),
    pincode VARCHAR(10),
    address_line1 VARCHAR(255),
    address_line2 VARCHAR(255),
    phone VARCHAR(20),
    email VARCHAR(100),
    customer_segment VARCHAR(50),
    customer_value_tier VARCHAR(20),
    acquisition_date DATE,
    acquisition_channel VARCHAR(50),
    is_active BOOLEAN DEFAULT TRUE,
    effective_start_date DATE,
    effective_end_date DATE,
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_customer_id (customer_id),
    INDEX idx_credit_score (credit_score),
    INDEX idx_city (city),
    INDEX idx_is_current (is_current),
    INDEX idx_income_tier (income_tier),
    INDEX idx_customer_segment (customer_segment),
    
    CONSTRAINT chk_customer_age CHECK (age >= 18 AND age <= 100),
    CONSTRAINT chk_credit_score CHECK (credit_score BETWEEN 300 AND 900),
    CONSTRAINT chk_annual_income CHECK (annual_income >= 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE dim_product (
    product_sk INT AUTO_INCREMENT PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL UNIQUE,
    product_name VARCHAR(100) NOT NULL,
    product_type VARCHAR(50) NOT NULL,
    loan_category VARCHAR(50),
    interest_rate_type VARCHAR(20) DEFAULT 'Fixed',
    min_interest_rate DECIMAL(5,2) NOT NULL,
    max_interest_rate DECIMAL(5,2) NOT NULL,
    min_loan_amount DECIMAL(15,2) NOT NULL,
    max_loan_amount DECIMAL(15,2) NOT NULL,
    min_tenure_months INT NOT NULL,
    max_tenure_months INT NOT NULL,
    processing_fee_percent DECIMAL(5,2) DEFAULT 1.0,
    prepayment_penalty BOOLEAN DEFAULT FALSE,
    prepayment_terms TEXT,
    collateral_required BOOLEAN DEFAULT FALSE,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_product_id (product_id),
    INDEX idx_product_type (product_type),
    INDEX idx_product_active (is_active),
    
    CONSTRAINT chk_interest_rate CHECK (min_interest_rate <= max_interest_rate),
    CONSTRAINT chk_loan_amount CHECK (min_loan_amount <= max_loan_amount),
    CONSTRAINT chk_tenure CHECK (min_tenure_months <= max_tenure_months),
    CONSTRAINT chk_fee_percent CHECK (processing_fee_percent >= 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE dim_branch (
    branch_sk INT AUTO_INCREMENT PRIMARY KEY,
    branch_id VARCHAR(50) NOT NULL UNIQUE,
    branch_name VARCHAR(100) NOT NULL,
    branch_type VARCHAR(50) DEFAULT 'Main',
    region VARCHAR(50),
    zone VARCHAR(50),
    state VARCHAR(50),
    city VARCHAR(100),
    pincode VARCHAR(10),
    address TEXT,
    manager_name VARCHAR(100),
    contact_number VARCHAR(20),
    email VARCHAR(100),
    opening_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_branch_id (branch_id),
    INDEX idx_region (region),
    INDEX idx_zone (zone),
    INDEX idx_branch_active (is_active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE dim_date (
    date_sk INT PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    day INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    quarter INT NOT NULL,
    year INT NOT NULL,
    week INT,
    weekday VARCHAR(20),
    is_weekend BOOLEAN DEFAULT FALSE,
    is_holiday BOOLEAN DEFAULT FALSE,
    financial_year VARCHAR(20),
    
    INDEX idx_full_date (full_date),
    INDEX idx_year_month (year, month),
    INDEX idx_year_quarter (year, quarter),
    INDEX idx_financial_year (financial_year)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE fact_loan (
    loan_sk BIGINT AUTO_INCREMENT PRIMARY KEY,
    loan_id VARCHAR(50) NOT NULL UNIQUE,
    customer_sk INT NOT NULL,
    product_sk INT NOT NULL,
    branch_sk INT NOT NULL,
    application_date_sk INT NOT NULL,
    disbursement_date_sk INT,
    first_emi_date_sk INT,
    loan_amount DECIMAL(15,2) NOT NULL,
    sanctioned_amount DECIMAL(15,2),
    interest_rate DECIMAL(5,2) NOT NULL,
    tenure_months INT NOT NULL,
    emi_amount DECIMAL(15,2),
    processing_fee DECIMAL(15,2),
    gst_on_fee DECIMAL(15,2),
    net_disbursed_amount DECIMAL(15,2),
    loan_purpose TEXT,
    collateral_id VARCHAR(50),
    collateral_value DECIMAL(15,2),
    loan_to_value_ratio DECIMAL(5,2),
    co_applicant_present BOOLEAN DEFAULT FALSE,
    co_applicant_income DECIMAL(15,2),
    bureau_score_at_origination INT,
    internal_risk_rating VARCHAR(10),
    probability_of_default DECIMAL(5,4) DEFAULT 0.0000,
    loss_given_default DECIMAL(5,4) DEFAULT 0.0000,
    exposure_at_default DECIMAL(15,2),
    expected_loss DECIMAL(15,2),
    current_balance DECIMAL(15,2),
    overdue_amount DECIMAL(15,2) DEFAULT 0,
    days_past_due INT DEFAULT 0,
    dpd_bucket VARCHAR(20) DEFAULT '0',
    npa_flag BOOLEAN DEFAULT FALSE,
    npa_date DATE,
    restructuring_flag BOOLEAN DEFAULT FALSE,
    restructuring_date DATE,
    written_off_flag BOOLEAN DEFAULT FALSE,
    written_off_date DATE,
    written_off_amount DECIMAL(15,2),
    loan_status VARCHAR(50) DEFAULT 'Active',
    foreclosure_date DATE,
    foreclosure_amount DECIMAL(15,2),
    fraud_flag BOOLEAN DEFAULT FALSE,
    fraud_type VARCHAR(50),
    fraud_detection_date DATE,
    collection_tier INT DEFAULT 1,
    assigned_collection_agent VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    FOREIGN KEY (customer_sk) REFERENCES dim_customer(customer_sk),
    FOREIGN KEY (product_sk) REFERENCES dim_product(product_sk),
    FOREIGN KEY (branch_sk) REFERENCES dim_branch(branch_sk),
    FOREIGN KEY (application_date_sk) REFERENCES dim_date(date_sk),
    FOREIGN KEY (disbursement_date_sk) REFERENCES dim_date(date_sk),
    FOREIGN KEY (first_emi_date_sk) REFERENCES dim_date(date_sk),
    
    INDEX idx_loan_id (loan_id),
    INDEX idx_customer_sk (customer_sk),
    INDEX idx_product_sk (product_sk),
    INDEX idx_loan_status (loan_status),
    INDEX idx_dpd_bucket (dpd_bucket),
    INDEX idx_npa_flag (npa_flag),
    INDEX idx_fraud_flag (fraud_flag),
    INDEX idx_disbursement_date (disbursement_date_sk),
    INDEX idx_loan_customer_status (customer_sk, loan_status, disbursement_date_sk),
    INDEX idx_loan_dpd_npa (dpd_bucket, npa_flag, disbursement_date_sk),
    INDEX idx_loan_fraud (fraud_flag, fraud_type, disbursement_date_sk),
    
    -- FIXED: Unique constraint names
    CONSTRAINT chk_loan_amount_positive CHECK (loan_amount > 0),
    CONSTRAINT chk_loan_interest_rate CHECK (interest_rate BETWEEN 5 AND 30),
    CONSTRAINT chk_loan_tenure CHECK (tenure_months BETWEEN 1 AND 360),
    CONSTRAINT chk_loan_dpd CHECK (days_past_due >= 0),
    CONSTRAINT chk_loan_pd CHECK (probability_of_default BETWEEN 0 AND 1),
    CONSTRAINT chk_loan_lgd CHECK (loss_given_default BETWEEN 0 AND 1),
    CONSTRAINT chk_loan_ltv CHECK (loan_to_value_ratio BETWEEN 0 AND 100)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE fact_transaction (
    transaction_sk BIGINT AUTO_INCREMENT PRIMARY KEY,
    transaction_id VARCHAR(50) NOT NULL UNIQUE,
    loan_sk BIGINT NOT NULL,
    customer_sk INT NOT NULL,
    transaction_date_sk INT NOT NULL,
    transaction_type VARCHAR(50) DEFAULT 'EMI',
    transaction_mode VARCHAR(50) DEFAULT 'NEFT',
    amount DECIMAL(15,2) NOT NULL,
    principal_component DECIMAL(15,2),
    interest_component DECIMAL(15,2),
    penalty_component DECIMAL(15,2),
    gst_component DECIMAL(15,2),
    payment_reference VARCHAR(100),
    bank_name VARCHAR(100),
    bank_account_last4 VARCHAR(10),
    transaction_status VARCHAR(20) DEFAULT 'Success',
    failure_reason TEXT,
    reconciliation_status VARCHAR(20) DEFAULT 'Pending',
    reconciled_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (loan_sk) REFERENCES fact_loan(loan_sk),
    FOREIGN KEY (customer_sk) REFERENCES dim_customer(customer_sk),
    FOREIGN KEY (transaction_date_sk) REFERENCES dim_date(date_sk),
    
    INDEX idx_transaction_id (transaction_id),
    INDEX idx_loan_sk (loan_sk),
    INDEX idx_transaction_date (transaction_date_sk),
    INDEX idx_transaction_type (transaction_type),
    INDEX idx_transaction_loan_date (loan_sk, transaction_date_sk, transaction_type),
    INDEX idx_transaction_status (transaction_status, reconciliation_status),
    
    CONSTRAINT chk_transaction_amount CHECK (amount > 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE fact_loan_daily_snapshot (
    snapshot_sk BIGINT AUTO_INCREMENT PRIMARY KEY,
    snapshot_date_sk INT NOT NULL,
    loan_sk BIGINT NOT NULL,
    customer_sk INT NOT NULL,
    product_sk INT NOT NULL,
    branch_sk INT NOT NULL,
    outstanding_principal DECIMAL(15,2),
    overdue_amount DECIMAL(15,2),
    days_past_due INT,
    dpd_bucket VARCHAR(20),
    npa_flag BOOLEAN,
    provision_required DECIMAL(15,2),
    interest_accrued DECIMAL(15,2),
    penal_interest_accrued DECIMAL(15,2),
    current_risk_rating VARCHAR(10),
    probability_of_default DECIMAL(5,4),
    loss_given_default DECIMAL(5,4),
    expected_credit_loss DECIMAL(15,2),
    collection_effort_score INT,
    fraud_risk_score INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (snapshot_date_sk) REFERENCES dim_date(date_sk),
    FOREIGN KEY (loan_sk) REFERENCES fact_loan(loan_sk),
    FOREIGN KEY (customer_sk) REFERENCES dim_customer(customer_sk),
    FOREIGN KEY (product_sk) REFERENCES dim_product(product_sk),
    FOREIGN KEY (branch_sk) REFERENCES dim_branch(branch_sk),
    
    INDEX idx_snapshot_date (snapshot_date_sk),
    INDEX idx_loan_sk (loan_sk),
    INDEX idx_dpd_bucket (dpd_bucket),
    INDEX idx_npa_flag (npa_flag),
    INDEX idx_fact_daily_snapshot_date_loan (snapshot_date_sk, loan_sk)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE fact_fraud_alert (
    alert_sk BIGINT AUTO_INCREMENT PRIMARY KEY,
    alert_id VARCHAR(50) NOT NULL UNIQUE,
    loan_sk BIGINT,
    customer_sk INT NOT NULL,
    transaction_sk BIGINT,
    detection_date_sk INT NOT NULL,
    alert_type VARCHAR(50),
    alert_category VARCHAR(50),
    risk_score INT DEFAULT 50,
    risk_level VARCHAR(20) DEFAULT 'Medium',
    detection_method VARCHAR(50),
    rule_triggered VARCHAR(100),
    alert_description TEXT,
    assigned_to VARCHAR(100),
    investigation_status VARCHAR(50) DEFAULT 'New',
    investigation_notes TEXT,
    resolution_date DATE,
    financial_impact DECIMAL(15,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    FOREIGN KEY (loan_sk) REFERENCES fact_loan(loan_sk),
    FOREIGN KEY (customer_sk) REFERENCES dim_customer(customer_sk),
    FOREIGN KEY (transaction_sk) REFERENCES fact_transaction(transaction_sk),
    FOREIGN KEY (detection_date_sk) REFERENCES dim_date(date_sk),
    
    INDEX idx_alert_id (alert_id),
    INDEX idx_customer_sk (customer_sk),
    INDEX idx_risk_level (risk_level),
    INDEX idx_investigation_status (investigation_status),
    INDEX idx_detection_date (detection_date_sk),
    INDEX idx_fraud_customer_date (customer_sk, detection_date_sk, risk_level),
    INDEX idx_fraud_alert_customer_status (customer_sk, investigation_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE etl_control (
    etl_id INT AUTO_INCREMENT PRIMARY KEY,
    etl_name VARCHAR(100) NOT NULL,
    table_name VARCHAR(100),
    last_run TIMESTAMP,
    next_run TIMESTAMP,
    status VARCHAR(20) DEFAULT 'Pending',
    records_processed INT DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_etl_name (etl_name),
    INDEX idx_status (status),
    INDEX idx_last_run (last_run)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE data_quality_rules (
    rule_id INT AUTO_INCREMENT PRIMARY KEY,
    rule_name VARCHAR(100) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    column_name VARCHAR(100),
    rule_type VARCHAR(50),
    rule_condition TEXT,
    severity VARCHAR(20) DEFAULT 'Error',
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_table_name (table_name),
    INDEX idx_rule_type (rule_type),
    INDEX idx_is_active (is_active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO dim_product (
    product_id, product_name, product_type, loan_category,
    min_interest_rate, max_interest_rate,
    min_loan_amount, max_loan_amount,
    min_tenure_months, max_tenure_months,
    processing_fee_percent, prepayment_penalty, collateral_required
) VALUES
-- Home Loans
('HL001', 'Home Loan Prime', 'Home Loan', 'Secured', 8.5, 10.5, 1000000, 10000000, 60, 240, 1.0, FALSE, TRUE),
('HL002', 'Home Loan Premium', 'Home Loan', 'Secured', 8.0, 9.5, 2000000, 20000000, 60, 300, 0.75, FALSE, TRUE),
('HL003', 'Home Loan Plus', 'Home Loan', 'Secured', 8.75, 11.0, 500000, 5000000, 36, 180, 1.25, TRUE, TRUE),

-- Auto Loans
('AL001', 'New Car Loan', 'Auto Loan', 'Secured', 9.5, 12.0, 200000, 3000000, 12, 84, 1.5, TRUE, TRUE),
('AL002', 'Used Car Loan', 'Auto Loan', 'Secured', 11.0, 14.0, 100000, 2000000, 12, 60, 2.0, TRUE, TRUE),
('AL003', 'Two Wheeler Loan', 'Auto Loan', 'Secured', 12.0, 16.0, 50000, 500000, 6, 48, 2.5, TRUE, TRUE),

-- Personal Loans
('PL001', 'Personal Loan Standard', 'Personal Loan', 'Unsecured', 11.0, 16.0, 50000, 1500000, 6, 60, 2.0, TRUE, FALSE),
('PL002', 'Personal Loan Premium', 'Personal Loan', 'Unsecured', 10.5, 14.0, 100000, 2500000, 12, 72, 1.75, TRUE, FALSE),
('PL003', 'Medical Emergency Loan', 'Personal Loan', 'Unsecured', 11.5, 15.0, 50000, 1000000, 6, 48, 1.5, FALSE, FALSE),

-- Business Loans
('BL001', 'Business Term Loan', 'Business Loan', 'Secured', 10.0, 14.0, 500000, 50000000, 12, 120, 1.25, TRUE, TRUE),
('BL002', 'Working Capital Loan', 'Business Loan', 'Secured', 11.0, 15.0, 100000, 20000000, 6, 60, 1.0, TRUE, TRUE),
('BL003', 'MSME Loan', 'Business Loan', 'Unsecured', 13.0, 18.0, 100000, 5000000, 6, 48, 2.0, TRUE, FALSE),

-- Education Loans
('EL001', 'Education Loan Domestic', 'Education Loan', 'Unsecured', 8.0, 11.0, 100000, 5000000, 12, 120, 0.5, FALSE, FALSE),
('EL002', 'Education Loan International', 'Education Loan', 'Secured', 8.5, 12.0, 500000, 20000000, 12, 180, 0.75, FALSE, TRUE);


INSERT INTO dim_branch (
    branch_id, branch_name, branch_type, region, zone, state, city, is_active
) VALUES
-- North Zone
('BR001', 'Delhi - Connaught Place', 'Main', 'North', 'North-1', 'Delhi', 'New Delhi', TRUE),
('BR002', 'Delhi - Nehru Place', 'Satellite', 'North', 'North-1', 'Delhi', 'New Delhi', TRUE),
('BR003', 'Gurgaon - Cyber City', 'Digital', 'North', 'North-1', 'Haryana', 'Gurgaon', TRUE),
('BR004', 'Noida - Sector 18', 'Satellite', 'North', 'North-1', 'Uttar Pradesh', 'Noida', TRUE),
('BR005', 'Lucknow - Hazratganj', 'Main', 'North', 'North-2', 'Uttar Pradesh', 'Lucknow', TRUE),
('BR006', 'Chandigarh - Sector 17', 'Main', 'North', 'North-2', 'Chandigarh', 'Chandigarh', TRUE),

-- South Zone
('BR007', 'Bangalore - MG Road', 'Main', 'South', 'South-1', 'Karnataka', 'Bangalore', TRUE),
('BR008', 'Bangalore - Electronic City', 'Satellite', 'South', 'South-1', 'Karnataka', 'Bangalore', TRUE),
('BR009', 'Chennai - Anna Salai', 'Main', 'South', 'South-2', 'Tamil Nadu', 'Chennai', TRUE),
('BR010', 'Hyderabad - Hitech City', 'Main', 'South', 'South-2', 'Telangana', 'Hyderabad', TRUE),
('BR011', 'Hyderabad - Banjara Hills', 'Satellite', 'South', 'South-2', 'Telangana', 'Hyderabad', TRUE),
('BR012', 'Kochi - MG Road', 'Main', 'South', 'South-3', 'Kerala', 'Kochi', TRUE),

-- West Zone
('BR013', 'Mumbai - Nariman Point', 'Main', 'West', 'West-1', 'Maharashtra', 'Mumbai', TRUE),
('BR014', 'Mumbai - Bandra Kurla', 'Main', 'West', 'West-1', 'Maharashtra', 'Mumbai', TRUE),
('BR015', 'Pune - Koregaon Park', 'Satellite', 'West', 'West-1', 'Maharashtra', 'Pune', TRUE),
('BR016', 'Ahmedabad - CG Road', 'Main', 'West', 'West-2', 'Gujarat', 'Ahmedabad', TRUE),
('BR017', 'Surat - Ring Road', 'Satellite', 'West', 'West-2', 'Gujarat', 'Surat', TRUE),

-- East Zone
('BR018', 'Kolkata - Chowringhee', 'Main', 'East', 'East-1', 'West Bengal', 'Kolkata', TRUE),
('BR019', 'Kolkata - Salt Lake', 'Satellite', 'East', 'East-1', 'West Bengal', 'Kolkata', TRUE),
('BR020', 'Patna - Frazer Road', 'Main', 'East', 'East-2', 'Bihar', 'Patna', TRUE),
('BR021', 'Bhubaneswar - Janpath', 'Main', 'East', 'East-2', 'Odisha', 'Bhubaneswar', TRUE),

-- Central Zone
('BR022', 'Bhopal - MP Nagar', 'Main', 'Central', 'Central-1', 'Madhya Pradesh', 'Bhopal', TRUE),
('BR023', 'Indore - MG Road', 'Satellite', 'Central', 'Central-1', 'Madhya Pradesh', 'Indore', TRUE),
('BR024', 'Jaipur - MI Road', 'Main', 'Central', 'Central-2', 'Rajasthan', 'Jaipur', TRUE),
('BR025', 'Lucknow - Gomti Nagar', 'Satellite', 'Central', 'Central-2', 'Uttar Pradesh', 'Lucknow', TRUE);


INSERT INTO data_quality_rules (
    rule_name, table_name, column_name, rule_type, rule_condition, severity
) VALUES
-- Customer rules
('Customer ID Not Null', 'dim_customer', 'customer_id', 'Not Null', 'IS NOT NULL', 'Error'),
('Valid Credit Score', 'dim_customer', 'credit_score', 'Range', 'BETWEEN 300 AND 900', 'Error'),
('Valid Age', 'dim_customer', 'age', 'Range', 'BETWEEN 18 AND 100', 'Error'),
('Valid Email', 'dim_customer', 'email', 'Format', 'REGEXP_LIKE(email, ''^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'')', 'Warning'),
('Valid Phone', 'dim_customer', 'phone', 'Format', 'REGEXP_LIKE(phone, ''^[0-9]{10}$'')', 'Warning'),

-- Loan rules
('Loan Amount Positive', 'fact_loan', 'loan_amount', 'Range', '> 0', 'Error'),
('Valid Interest Rate', 'fact_loan', 'interest_rate', 'Range', 'BETWEEN 5 AND 30', 'Error'),
('Valid Tenure', 'fact_loan', 'tenure_months', 'Range', 'BETWEEN 1 AND 360', 'Error'),
('Valid DPD', 'fact_loan', 'days_past_due', 'Range', '>= 0', 'Error'),
('Valid PD', 'fact_loan', 'probability_of_default', 'Range', 'BETWEEN 0 AND 1', 'Warning'),

-- Transaction rules
('Transaction Amount Positive', 'fact_transaction', 'amount', 'Range', '> 0', 'Error'),
('Valid Transaction Date', 'fact_transaction', 'transaction_date_sk', 'Not Null', 'IS NOT NULL', 'Error');


DELIMITER $$

CREATE PROCEDURE sp_fill_date_dimension(IN start_date DATE, IN end_date DATE)
BEGIN
    DECLARE v_current_date DATE;
    SET v_current_date = start_date;
    
    WHILE v_current_date <= end_date DO
        INSERT IGNORE INTO dim_date (
            date_sk, full_date, day, month, month_name, 
            quarter, year, week, weekday, is_weekend, financial_year
        ) VALUES (
            YEAR(v_current_date) * 10000 + MONTH(v_current_date) * 100 + DAY(v_current_date),
            v_current_date,
            DAY(v_current_date),
            MONTH(v_current_date),
            MONTHNAME(v_current_date),
            QUARTER(v_current_date),
            YEAR(v_current_date),
            WEEK(v_current_date, 1),
            DAYNAME(v_current_date),
            CASE WHEN DAYOFWEEK(v_current_date) IN (1,7) THEN TRUE ELSE FALSE END,
            CONCAT('FY', 
                CASE 
                    WHEN MONTH(v_current_date) >= 4 
                    THEN YEAR(v_current_date) 
                    ELSE YEAR(v_current_date) - 1 
                END, 
                '-', 
                CASE 
                    WHEN MONTH(v_current_date) >= 4 
                    THEN YEAR(v_current_date) + 1 
                    ELSE YEAR(v_current_date) 
                END
            )
        );
        SET v_current_date = DATE_ADD(v_current_date, INTERVAL 1 DAY);
    END WHILE;
END$$



CREATE TRIGGER trg_update_dpd_bucket 
BEFORE UPDATE ON fact_loan
FOR EACH ROW
BEGIN
    IF NEW.days_past_due = 0 THEN
        SET NEW.dpd_bucket = '0';
    ELSEIF NEW.days_past_due <= 30 THEN
        SET NEW.dpd_bucket = '1-30';
    ELSEIF NEW.days_past_due <= 60 THEN
        SET NEW.dpd_bucket = '31-60';
    ELSEIF NEW.days_past_due <= 90 THEN
        SET NEW.dpd_bucket = '61-90';
    ELSE
        SET NEW.dpd_bucket = '90+';
    END IF;
    
    -- Auto-update NPA flag
    IF NEW.days_past_due > 90 THEN
        SET NEW.npa_flag = TRUE;
        IF NEW.npa_date IS NULL THEN
            SET NEW.npa_date = CURDATE();
        END IF;
    END IF;
END$$


CREATE TRIGGER trg_update_loan_balance
AFTER INSERT ON fact_transaction
FOR EACH ROW
BEGIN
    DECLARE v_remaining_balance DECIMAL(15,2);
    
    IF NEW.transaction_type IN ('EMI', 'Prepayment', 'Foreclosure') AND 
       NEW.transaction_status = 'Success' THEN
        
        UPDATE fact_loan 
        SET current_balance = current_balance - NEW.principal_component,
            updated_at = CURRENT_TIMESTAMP
        WHERE loan_sk = NEW.loan_sk;
        
        -- Update loan status if fully paid
        SELECT current_balance INTO v_remaining_balance
        FROM fact_loan 
        WHERE loan_sk = NEW.loan_sk;
        
        IF v_remaining_balance <= 0 THEN
            UPDATE fact_loan 
            SET loan_status = 'Closed',
                updated_at = CURRENT_TIMESTAMP
            WHERE loan_sk = NEW.loan_sk;
        END IF;
    END IF;
END$$

DELIMITER ;


CALL sp_fill_date_dimension('2022-01-01', '2026-12-31');


CREATE OR REPLACE VIEW vw_active_loans AS
SELECT 
    l.loan_id,
    c.customer_id,
    CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
    p.product_name,
    l.loan_amount,
    l.interest_rate,
    l.current_balance,
    l.emi_amount,
    l.days_past_due,
    l.dpd_bucket,
    l.npa_flag,
    l.loan_status,
    d.full_date AS disbursement_date,
    b.branch_name,
    b.region
FROM fact_loan l
JOIN dim_customer c ON l.customer_sk = c.customer_sk
JOIN dim_product p ON l.product_sk = p.product_sk
JOIN dim_branch b ON l.branch_sk = b.branch_sk
JOIN dim_date d ON l.disbursement_date_sk = d.date_sk
WHERE l.loan_status IN ('Active', 'Overdue');


CREATE OR REPLACE VIEW vw_npa_summary AS
SELECT 
    d.year,
    d.month,
    d.month_name,
    p.product_type,
    COUNT(DISTINCT l.loan_id) AS npa_count,
    SUM(l.current_balance) AS npa_amount,
    AVG(l.days_past_due) AS avg_dpd,
    SUM(l.expected_loss) AS expected_loss
FROM fact_loan l
JOIN dim_date d ON l.disbursement_date_sk = d.date_sk
JOIN dim_product p ON l.product_sk = p.product_sk
WHERE l.npa_flag = TRUE
GROUP BY d.year, d.month, d.month_name, p.product_type
ORDER BY d.year DESC, d.month DESC;


CREATE OR REPLACE VIEW vw_fraud_summary AS
SELECT 
    d.year,
    d.month,
    d.month_name,
    fa.alert_type,
    fa.risk_level,
    COUNT(*) AS alert_count,
    SUM(fa.financial_impact) AS total_financial_impact,
    COUNT(CASE WHEN fa.investigation_status = 'Confirmed' THEN 1 END) AS confirmed_cases
FROM fact_fraud_alert fa
JOIN dim_date d ON fa.detection_date_sk = d.date_sk
GROUP BY d.year, d.month, d.month_name, fa.alert_type, fa.risk_level
ORDER BY d.year DESC, d.month DESC;


CREATE OR REPLACE VIEW vw_portfolio_performance AS
SELECT 
    d.year,
    d.month,
    p.product_type,
    COUNT(l.loan_id) AS loans_originated,
    SUM(l.loan_amount) AS disbursement_amount,
    AVG(l.interest_rate) AS avg_interest_rate,
    SUM(CASE WHEN l.npa_flag = 1 THEN 1 ELSE 0 END) / COUNT(l.loan_id) * 100 AS npa_percentage,
    SUM(t.amount) AS total_collection,
    SUM(t.amount) / NULLIF(SUM(l.loan_amount), 0) * 100 AS collection_efficiency
FROM dim_date d
LEFT JOIN fact_loan l ON d.date_sk = l.disbursement_date_sk
LEFT JOIN dim_product p ON l.product_sk = p.product_sk
LEFT JOIN fact_transaction t ON d.date_sk = t.transaction_date_sk
GROUP BY d.year, d.month, p.product_type
ORDER BY d.year DESC, d.month DESC;


CREATE OR REPLACE VIEW vw_branch_performance AS
SELECT 
    b.region,
    b.zone,
    b.branch_name,
    b.city,
    COUNT(DISTINCT l.loan_id) AS total_loans,
    SUM(l.loan_amount) AS total_disbursed,
    SUM(l.current_balance) AS outstanding,
    COUNT(DISTINCT CASE WHEN l.npa_flag = 1 THEN l.loan_id END) AS npa_loans,
    COUNT(DISTINCT CASE WHEN l.fraud_flag = 1 THEN l.loan_id END) AS fraud_cases
FROM dim_branch b
LEFT JOIN fact_loan l ON b.branch_sk = l.branch_sk
WHERE b.is_active = TRUE
GROUP BY b.region, b.zone, b.branch_name, b.city;



SELECT '✅ DATABASE CREATED SUCCESSFULLY!' AS status;
SELECT CONCAT('📊 Total Tables: ', COUNT(*)) AS summary 
FROM information_schema.tables 
WHERE table_schema = 'creditflow360';

SELECT '🎯 CREDITFLOW360 IS READY FOR DATA GENERATION!' AS message;