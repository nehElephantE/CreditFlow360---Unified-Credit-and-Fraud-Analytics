import pandas as pd
import numpy as np
from datetime import datetime
import logging
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataValidator:
    @staticmethod
    def validate_customers(df: pd.DataFrame) -> dict:
        logger.info("🔍 Validating customer data...")
        
        results = {
            'table': 'dim_customer',
            'total_records': len(df),
            'checks': {},
            'warnings': [],
            'errors': [],
            'pass_rate': 0
        }
        
        missing = df.isnull().sum().to_dict()
        results['checks']['missing_values'] = missing
        
        critical_fields = ['customer_id', 'first_name', 'last_name', 'date_of_birth']
        for field in critical_fields:
            if field in missing and missing[field] > 0:
                results['errors'].append(f"Missing values in {field}: {missing[field]}")
        
        duplicate_ids = df['customer_id'].duplicated().sum()
        results['checks']['duplicate_customer_ids'] = duplicate_ids
        if duplicate_ids > 0:
            results['errors'].append(f"Duplicate customer IDs: {duplicate_ids}")
        
        invalid_credit = ((df['credit_score'] < 300) | (df['credit_score'] > 900)).sum()
        results['checks']['invalid_credit_scores'] = invalid_credit
        if invalid_credit > 0:
            results['errors'].append(f"Invalid credit scores: {invalid_credit}")
        
        invalid_age = ((df['age'] < 18) | (df['age'] > 100)).sum()
        results['checks']['invalid_age'] = invalid_age
        if invalid_age > 0:
            results['errors'].append(f"Invalid age: {invalid_age}")
        
        negative_income = (df['annual_income'] < 0).sum()
        results['checks']['negative_income'] = negative_income
        if negative_income > 0:
            results['errors'].append(f"Negative income: {negative_income}")
        
        invalid_email = df['email'].str.contains('@').sum() != len(df)
        results['checks']['invalid_email'] = not invalid_email
        
        total_checks = len(results['checks'])
        error_count = len(results['errors'])
        results['pass_rate'] = 1 - (error_count / max(total_checks, 1))
        
        logger.info(f"✅ Customer validation complete. Pass rate: {results['pass_rate']:.1%}")
        return results
    
    @staticmethod
    def validate_loans(df: pd.DataFrame) -> dict:
        logger.info("🔍 Validating loan data...")
        
        results = {
            'table': 'fact_loan',
            'total_records': len(df),
            'checks': {},
            'warnings': [],
            'errors': [],
            'pass_rate': 0
        }
        
        # Filter only approved/disbursed loans
        approved_loans = df[df['disbursement_date'].notna()]
        
        # Check 1: Missing foreign keys
        missing_customer = df['customer_id'].isnull().sum()
        results['checks']['missing_customer_id'] = missing_customer
        if missing_customer > 0:
            results['errors'].append(f"Missing customer ID: {missing_customer}")
        
        # Check 2: Loan amount
        zero_loan = (df['loan_amount'] <= 0).sum()
        results['checks']['zero_loan_amount'] = zero_loan
        if zero_loan > 0:
            results['errors'].append(f"Zero or negative loan amount: {zero_loan}")
        
        # Check 3: Interest rate
        if len(approved_loans) > 0:
            invalid_rate = ((approved_loans['interest_rate'] < 5) | 
                          (approved_loans['interest_rate'] > 30)).sum()
            results['checks']['invalid_interest_rate'] = invalid_rate
            if invalid_rate > 0:
                results['errors'].append(f"Invalid interest rate: {invalid_rate}")
        
        # Check 4: Tenure
        if len(approved_loans) > 0:
            invalid_tenure = ((approved_loans['tenure_months'] < 1) | 
                            (approved_loans['tenure_months'] > 360)).sum()
            results['checks']['invalid_tenure'] = invalid_tenure
            if invalid_tenure > 0:
                results['errors'].append(f"Invalid tenure: {invalid_tenure}")
        
        # Check 5: DPD consistency
        dpd_mismatch = ((df['days_past_due'] == 0) & (df['dpd_bucket'] != '0')).sum()
        results['checks']['dpd_bucket_mismatch'] = dpd_mismatch
        if dpd_mismatch > 0:
            results['warnings'].append(f"DPD bucket mismatch: {dpd_mismatch}")
        
        # Check 6: NPA flag
        npa_mismatch = ((df['days_past_due'] > 90) & (df['npa_flag'] == False)).sum()
        results['checks']['npa_flag_mismatch'] = npa_mismatch
        if npa_mismatch > 0:
            results['warnings'].append(f"NPA flag mismatch: {npa_mismatch}")
        
        # Calculate pass rate
        total_checks = len(results['checks'])
        error_count = len(results['errors'])
        results['pass_rate'] = 1 - (error_count / max(total_checks, 1))
        
        logger.info(f"✅ Loan validation complete. Pass rate: {results['pass_rate']:.1%}")
        return results
    
    @staticmethod
    def validate_transactions(df: pd.DataFrame) -> dict:
        logger.info("🔍 Validating transaction data...")
        
        results = {
            'table': 'fact_transaction',
            'total_records': len(df),
            'checks': {},
            'warnings': [],
            'errors': [],
            'pass_rate': 0
        }
        
        # Check 1: Missing references
        missing_loan = df['loan_id'].isnull().sum()
        results['checks']['missing_loan_id'] = missing_loan
        if missing_loan > 0:
            results['errors'].append(f"Missing loan ID: {missing_loan}")
        
        # Check 2: Transaction amount
        zero_amount = (df['amount'] <= 0).sum()
        results['checks']['zero_amount'] = zero_amount
        if zero_amount > 0:
            results['errors'].append(f"Zero or negative amount: {zero_amount}")
        
        # Check 3: Invalid transaction type
        valid_types = ['EMI', 'Prepayment', 'Foreclosure', 'Disbursement', 'Penalty', 'Processing Fee']
        invalid_type = df[~df['transaction_type'].isin(valid_types)].shape[0]
        results['checks']['invalid_transaction_type'] = invalid_type
        if invalid_type > 0:
            results['errors'].append(f"Invalid transaction type: {invalid_type}")
        
        # Check 4: Transaction status
        valid_status = ['Success', 'Failed', 'Pending']
        invalid_status = df[~df['transaction_status'].isin(valid_status)].shape[0]
        results['checks']['invalid_status'] = invalid_status
        if invalid_status > 0:
            results['errors'].append(f"Invalid transaction status: {invalid_status}")
        
        # Calculate pass rate
        total_checks = len(results['checks'])
        error_count = len(results['errors'])
        results['pass_rate'] = 1 - (error_count / max(total_checks, 1))
        
        logger.info(f"✅ Transaction validation complete. Pass rate: {results['pass_rate']:.1%}")
        return results
    
    @staticmethod
    def validate_fraud_alerts(df: pd.DataFrame) -> dict:
        logger.info("🔍 Validating fraud alert data...")
        
        results = {
            'table': 'fact_fraud_alert',
            'total_records': len(df),
            'checks': {},
            'warnings': [],
            'errors': [],
            'pass_rate': 0
        }
        
        if len(df) == 0:
            results['warnings'].append("No fraud alerts generated")
            return results
        
        # Check 1: Risk score range
        invalid_score = ((df['risk_score'] < 1) | (df['risk_score'] > 100)).sum()
        results['checks']['invalid_risk_score'] = invalid_score
        if invalid_score > 0:
            results['errors'].append(f"Invalid risk score: {invalid_score}")
        
        # Check 2: Risk level consistency
        risk_mapping = {
            'Critical': (80, 100),
            'High': (60, 79),
            'Medium': (40, 59),
            'Low': (1, 39)
        }
        
        inconsistent = 0
        for _, row in df.iterrows():
            if row['risk_level'] in risk_mapping:
                min_score, max_score = risk_mapping[row['risk_level']]
                if not (min_score <= row['risk_score'] <= max_score):
                    inconsistent += 1
        
        results['checks']['inconsistent_risk_level'] = inconsistent
        if inconsistent > 0:
            results['warnings'].append(f"Inconsistent risk level: {inconsistent}")
        
        # Check 3: Investigation status
        valid_status = ['New', 'In Progress', 'Confirmed', 'False Positive']
        invalid_status = df[~df['investigation_status'].isin(valid_status)].shape[0]
        results['checks']['invalid_status'] = invalid_status
        if invalid_status > 0:
            results['errors'].append(f"Invalid investigation status: {invalid_status}")
        
        # Calculate pass rate
        total_checks = len(results['checks'])
        error_count = len(results['errors'])
        results['pass_rate'] = 1 - (error_count / max(total_checks, 1))
        
        logger.info(f"✅ Fraud alert validation complete. Pass rate: {results['pass_rate']:.1%}")
        return results
    
    @staticmethod
    def generate_quality_report(customers: pd.DataFrame, loans: pd.DataFrame,
                               transactions: pd.DataFrame, fraud_alerts: pd.DataFrame) -> dict:
        
        report = {
            'generated_at': datetime.now().isoformat(),
            'summary': {
                'customers': len(customers),
                'loans': len(loans),
                'transactions': len(transactions),
                'fraud_alerts': len(fraud_alerts),
                'approved_loans': loans[loans['disbursement_date'].notna()].shape[0] if len(loans) > 0 else 0,
                'rejected_loans': loans[loans['loan_status'] == 'Rejected'].shape[0] if len(loans) > 0 else 0,
                'active_loans': loans[loans['loan_status'].isin(['Active', 'Overdue'])].shape[0] if len(loans) > 0 else 0,
                'npa_loans': loans[loans['npa_flag'] == True].shape[0] if len(loans) > 0 else 0,
                'fraud_loans': loans[loans['fraud_flag'] == True].shape[0] if len(loans) > 0 else 0
            },
            'quality_checks': {
                'customers': DataValidator.validate_customers(customers),
                'loans': DataValidator.validate_loans(loans),
                'transactions': DataValidator.validate_transactions(transactions),
                'fraud_alerts': DataValidator.validate_fraud_alerts(fraud_alerts)
            }
        }
        
        scores = []
        for table, result in report['quality_checks'].items():
            if result and 'pass_rate' in result:
                scores.append(result['pass_rate'])
        
        report['overall_quality_score'] = float(np.mean(scores)) if scores else 0.0
        
        report_path = f'data/quality_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        logger.info(f"💾 Quality report saved to {report_path}")
        
        return report