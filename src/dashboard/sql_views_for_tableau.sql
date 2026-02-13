CREATE OR REPLACE VIEW vw_executive_dashboard AS
SELECT 
    CURRENT_DATE() as report_date,
    -- Portfolio Summary
    (SELECT SUM(current_balance) FROM fact_loan WHERE loan_status IN ('Active', 'Overdue')) as total_outstanding,
    (SELECT COUNT(*) FROM fact_loan WHERE disbursement_date >= DATE_SUB(NOW(), INTERVAL 30 DAY)) as new_loans_30d,
    (SELECT SUM(loan_amount) FROM fact_loan WHERE disbursement_date >= DATE_SUB(NOW(), INTERVAL 30 DAY)) as new_disbursements_30d,
    
    -- Risk Metrics
    (SELECT SUM(current_balance) FROM fact_loan WHERE npa_flag = 1) / 
    NULLIF((SELECT SUM(current_balance) FROM fact_loan WHERE loan_status IN ('Active', 'Overdue', 'NPA')), 0) as gnpa_ratio,
    
    (SELECT AVG(probability_of_default) FROM fact_loan WHERE loan_status IN ('Active', 'Overdue')) as avg_pd,
    (SELECT AVG(loss_given_default) FROM fact_loan WHERE loan_status IN ('Active', 'Overdue')) as avg_lgd,
    
    -- Fraud Metrics
    (SELECT COUNT(*) FROM fact_fraud_alert WHERE detection_date >= DATE_SUB(NOW(), INTERVAL 7 DAY)) as fraud_alerts_7d,
    (SELECT COUNT(*) FROM fact_fraud_alert WHERE investigation_status = 'Confirmed' AND detection_date >= DATE_SUB(NOW(), INTERVAL 30 DAY)) as confirmed_fraud_30d,
    
    -- Collection Metrics
    (SELECT SUM(overdue_amount) FROM fact_loan WHERE days_past_due BETWEEN 1 AND 30) as bucket_1_amount,
    (SELECT SUM(overdue_amount) FROM fact_loan WHERE days_past_due BETWEEN 31 AND 60) as bucket_2_amount,
    (SELECT SUM(overdue_amount) FROM fact_loan WHERE days_past_due BETWEEN 61 AND 90) as bucket_3_amount,
    (SELECT SUM(overdue_amount) FROM fact_loan WHERE days_past_due > 90) as bucket_4_amount;

CREATE OR REPLACE VIEW vw_portfolio_risk_heatmap AS
SELECT 
    c.credit_tier,
    p.product_type,
    COUNT(l.loan_id) as loan_count,
    SUM(l.current_balance) as exposure,
    AVG(l.probability_of_default) as pd_rate,
    AVG(l.loss_given_default) as lgd_rate,
    SUM(l.current_balance * l.probability_of_default * l.loss_given_default) as expected_loss,
    SUM(CASE WHEN l.days_past_due > 30 THEN 1 ELSE 0 END) / COUNT(l.loan_id) as delinquency_rate,
    SUM(CASE WHEN l.npa_flag = 1 THEN 1 ELSE 0 END) / COUNT(l.loan_id) as npa_rate,
    CASE 
        WHEN AVG(l.probability_of_default) < 0.05 AND SUM(CASE WHEN l.npa_flag = 1 THEN 1 ELSE 0 END) / COUNT(l.loan_id) < 0.02 THEN 'Low Risk'
        WHEN AVG(l.probability_of_default) < 0.15 AND SUM(CASE WHEN l.npa_flag = 1 THEN 1 ELSE 0 END) / COUNT(l.loan_id) < 0.05 THEN 'Medium Risk'
        ELSE 'High Risk'
    END as risk_category
FROM fact_loan l
JOIN dim_customer c ON l.customer_sk = c.customer_sk
JOIN dim_product p ON l.product_sk = p.product_sk
WHERE l.loan_status IN ('Active', 'Overdue', 'NPA')
GROUP BY c.credit_tier, p.product_type;

CREATE OR REPLACE VIEW vw_fraud_dashboard AS
SELECT 
    DATE(fa.detection_date) as detection_day,
    fa.alert_type,
    fa.risk_level,
    COUNT(*) as alert_count,
    SUM(CASE WHEN fa.investigation_status = 'Confirmed' THEN 1 ELSE 0 END) as confirmed_count,
    SUM(fa.financial_impact) as total_financial_impact,
    AVG(fa.risk_score) as avg_risk_score,
    COUNT(DISTINCT fa.customer_sk) as unique_customers,
    GROUP_CONCAT(DISTINCT c.credit_tier) as credit_tiers_affected
FROM fact_fraud_alert fa
JOIN dim_customer c ON fa.customer_sk = c.customer_sk
WHERE fa.detection_date >= DATE_SUB(NOW(), INTERVAL 90 DAY)
GROUP BY DATE(fa.detection_date), fa.alert_type, fa.risk_level
ORDER BY detection_day DESC;

CREATE OR REPLACE VIEW vw_collection_performance AS
SELECT 
    l.collection_tier,
    COUNT(DISTINCT l.loan_id) as assigned_loans,
    SUM(l.overdue_amount) as total_overdue,
    SUM(t.amount) as amount_collected,
    COUNT(DISTINCT t.transaction_id) as collection_attempts,
    SUM(t.amount) / NULLIF(SUM(l.overdue_amount), 0) as collection_efficiency,
    AVG(DATEDIFF(t.transaction_date, l.first_emi_date)) as avg_days_to_collect
FROM fact_loan l
LEFT JOIN fact_transaction t ON l.loan_sk = t.loan_sk 
    AND t.transaction_type = 'EMI'
    AND t.transaction_date >= DATE_SUB(NOW(), INTERVAL 30 DAY)
WHERE l.days_past_due > 0
GROUP BY l.collection_tier;

CREATE OR REPLACE VIEW vw_regional_performance AS
SELECT 
    b.region,
    b.zone,
    COUNT(DISTINCT l.loan_id) as loan_volume,
    SUM(l.loan_amount) as disbursement_amount,
    AVG(l.interest_rate) as avg_rate,
    SUM(CASE WHEN l.npa_flag = 1 THEN l.current_balance ELSE 0 END) / NULLIF(SUM(l.current_balance), 0) as regional_npa,
    COUNT(DISTINCT CASE WHEN l.fraud_flag = 1 THEN l.loan_id END) as fraud_cases
FROM fact_loan l
JOIN dim_branch b ON l.branch_sk = b.branch_sk
WHERE l.disbursement_date >= DATE_SUB(NOW(), INTERVAL 1 YEAR)
GROUP BY b.region, b.zone;

CREATE OR REPLACE VIEW vw_customer_segmentation AS
SELECT 
    c.customer_segment,
    c.credit_tier,
    c.income_tier,
    COUNT(DISTINCT c.customer_sk) as customer_count,
    AVG(c.credit_score) as avg_credit_score,
    AVG(c.annual_income) as avg_income,
    COUNT(DISTINCT l.loan_id) / NULLIF(COUNT(DISTINCT c.customer_sk), 0) as loans_per_customer,
    AVG(l.loan_amount) as avg_loan_amount,
    AVG(l.interest_rate) as avg_interest_rate,
    SUM(CASE WHEN l.npa_flag = 1 THEN 1 ELSE 0 END) / NULLIF(COUNT(DISTINCT l.loan_id), 0) as default_rate
FROM dim_customer c
LEFT JOIN fact_loan l ON c.customer_sk = l.customer_sk
WHERE c.is_current = 1
GROUP BY c.customer_segment, c.credit_tier, c.income_tier;

CREATE OR REPLACE VIEW vw_monthly_trends AS
SELECT 
    d.year,
    d.month,
    d.month_name,
    d.quarter,
    COUNT(l.loan_id) as loans_originated,
    SUM(l.loan_amount) as disbursement_volume,
    SUM(t.amount) as collection_volume,
    SUM(t.amount) / NULLIF(SUM(l.loan_amount), 0) as collection_ratio,
    COUNT(CASE WHEN l.npa_flag = 1 THEN 1 END) as new_npa_count,
    SUM(CASE WHEN l.npa_flag = 1 THEN l.current_balance END) as new_npa_amount
FROM dim_date d
LEFT JOIN fact_loan l ON d.date_sk = l.disbursement_date_sk
LEFT JOIN fact_transaction t ON d.date_sk = t.transaction_date_sk
GROUP BY d.year, d.month, d.month_name, d.quarter
ORDER BY d.year DESC, d.month DESC;

CREATE OR REPLACE VIEW vw_regulatory_compliance AS
SELECT 
    'Capital Adequacy' as metric_name,
    CONCAT(ROUND(SUM(CASE WHEN dpd_bucket = '0' THEN current_balance * 0.20 ELSE 0 END) * 0.20 / 
           NULLIF(SUM(current_balance), 0) * 100, 2), '%') as current_value,
    '15%' as regulatory_requirement,
    CASE 
        WHEN SUM(CASE WHEN dpd_bucket = '0' THEN current_balance * 0.20 ELSE 0 END) * 0.20 / 
             NULLIF(SUM(current_balance), 0) * 100 >= 15 
        THEN 'Compliant' 
        ELSE 'Breach' 
    END as status
FROM fact_loan
WHERE loan_status IN ('Active', 'Overdue', 'NPA')

UNION ALL

SELECT 
    'NPA Coverage' as metric_name,
    CONCAT(ROUND(SUM(CASE WHEN npa_flag = 1 THEN current_balance * 0.40 ELSE 0 END) / 
           NULLIF(SUM(CASE WHEN npa_flag = 1 THEN current_balance ELSE 0 END), 0) * 100, 2), '%') as current_value,
    '40%' as regulatory_requirement,
    CASE 
        WHEN SUM(CASE WHEN npa_flag = 1 THEN current_balance * 0.40 ELSE 0 END) / 
             NULLIF(SUM(CASE WHEN npa_flag = 1 THEN current_balance ELSE 0 END), 0) * 100 >= 40 
        THEN 'Compliant' 
        ELSE 'Breach' 
    END as status
FROM fact_loan;