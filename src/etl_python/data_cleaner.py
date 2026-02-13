import pandas as pd
import numpy as np
from datetime import datetime
import logging
from typing import Dict, List, Tuple, Optional
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataCleaner:
    @staticmethod
    def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Cleaning customer data...")
        df_clean = df.copy()
        
        if df_clean.empty:
            logger.warning("Empty customer dataframe received")
            return df_clean
        
        if 'email' in df_clean.columns:
            df_clean['email'] = df_clean['email'].fillna('unknown@email.com')
        if 'phone' in df_clean.columns:
            df_clean['phone'] = df_clean['phone'].fillna('0000000000')
        
        date_columns = ['date_of_birth', 'acquisition_date', 'effective_start_date', 'effective_end_date']
        for col in date_columns:
            if col in df_clean.columns:
                df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
        
        if 'email' in df_clean.columns:
            df_clean['email'] = df_clean['email'].str.lower().str.strip()
            df_clean['email'] = df_clean['email'].apply(
                lambda x: x if '@' in str(x) else f"{str(x)[:10]}@email.com"
            )
        
        if 'phone' in df_clean.columns:
            df_clean['phone'] = df_clean['phone'].astype(str).str.replace('[^0-9]', '', regex=True)
            df_clean['phone'] = df_clean['phone'].str[-10:]  # Last 10 digits
            df_clean.loc[df_clean['phone'].str.len() != 10, 'phone'] = '9999999999'
        
        if 'gender' in df_clean.columns:
            df_clean['gender'] = df_clean['gender'].str.capitalize()
        if 'marital_status' in df_clean.columns:
            df_clean['marital_status'] = df_clean['marital_status'].str.capitalize()
        if 'employment_type' in df_clean.columns:
            df_clean['employment_type'] = df_clean['employment_type'].str.replace('_', ' ').str.title()
        
        if 'annual_income' in df_clean.columns:
            df_clean['annual_income'] = df_clean['annual_income'].clip(upper=100000000)  # Max 10Cr
        if 'credit_score' in df_clean.columns:
            df_clean['credit_score'] = df_clean['credit_score'].clip(300, 900)
        
        if 'acquisition_date' in df_clean.columns and 'date_of_birth' in df_clean.columns:
            df_clean['age_at_acquisition'] = (
                pd.to_datetime(df_clean['acquisition_date']).dt.year - 
                pd.to_datetime(df_clean['date_of_birth']).dt.year
            )
        
        if 'annual_income' in df_clean.columns:
            df_clean['income_per_month'] = df_clean['annual_income'] / 12
        
        if 'credit_score' in df_clean.columns:
            df_clean['credit_score_category'] = pd.cut(
                df_clean['credit_score'],
                bins=[0, 550, 650, 750, 900],
                labels=['Poor', 'Fair', 'Good', 'Excellent'],
                include_lowest=True
            )
        
        if 'customer_id' in df_clean.columns:
            df_clean = df_clean.drop_duplicates(subset=['customer_id'], keep='last')
        
        logger.info(f"Customer cleaning complete. Shape: {df_clean.shape}")
        return df_clean
    
    @staticmethod
    def clean_loans(df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize loan data"""
        logger.info("Cleaning loan data...")
        df_clean = df.copy()
        
        if df_clean.empty:
            logger.warning("Empty loan dataframe received")
            return df_clean
        
        date_columns = ['application_date', 'disbursement_date', 'first_emi_date', 
                       'fraud_detection_date', 'npa_date']
        for col in date_columns:
            if col in df_clean.columns:
                df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
        
        numeric_columns = ['loan_amount', 'sanctioned_amount', 'interest_rate', 
                          'emi_amount', 'collateral_value', 'current_balance',
                          'overdue_amount', 'probability_of_default', 'loss_given_default']
        
        for col in numeric_columns:
            if col in df_clean.columns:
                df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
                df_clean[col] = df_clean[col].fillna(0)
                df_clean[col] = df_clean[col].clip(lower=0)
        
        if 'interest_rate' in df_clean.columns:
            df_clean['interest_rate'] = df_clean['interest_rate'].clip(5, 25)
        
        if 'loan_amount' in df_clean.columns:
            df_clean['loan_amount'] = df_clean['loan_amount'].clip(lower=5000)  # Minimum 5000 loan
        
        if 'loan_status' in df_clean.columns and 'days_past_due' in df_clean.columns:
            df_clean.loc[df_clean['loan_status'] == 'Active', 'days_past_due'] = 0
            df_clean.loc[df_clean['days_past_due'] < 0, 'days_past_due'] = 0
        
        if 'days_past_due' in df_clean.columns:
            df_clean['dpd_bucket'] = df_clean['days_past_due'].apply(
                DataCleaner._get_dpd_bucket_consistent
            )
        
        if 'days_past_due' in df_clean.columns:
            df_clean['npa_flag'] = df_clean['days_past_due'] > 90
        
        if 'disbursement_date' in df_clean.columns:
            df_clean['loan_age_days'] = (
                datetime.now() - pd.to_datetime(df_clean['disbursement_date'])
            ).dt.days
            df_clean['loan_age_months'] = df_clean['loan_age_days'] / 30
        
        if 'loan_amount' in df_clean.columns and 'customer_id' in df_clean.columns:
            df_clean = df_clean[
                (df_clean['loan_amount'] > 0) & 
                (df_clean['customer_id'].notna())
            ]
        
        logger.info(f"Loan cleaning complete. Shape: {df_clean.shape}")
        return df_clean
    
    @staticmethod
    def clean_transactions(df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Cleaning transaction data...")
        df_clean = df.copy()
        
        if df_clean.empty:
            logger.warning("Empty transaction dataframe received")
            return df_clean
        
        if 'transaction_date' in df_clean.columns:
            df_clean['transaction_date'] = pd.to_datetime(df_clean['transaction_date'], errors='coerce')
        
        if 'amount' in df_clean.columns:
            df_clean['amount'] = pd.to_numeric(df_clean['amount'], errors='coerce').fillna(0)
            df_clean['amount'] = df_clean['amount'].clip(lower=1)  # Minimum ₹1 transaction
        
        if 'transaction_type' in df_clean.columns:
            df_clean['transaction_type'] = df_clean['transaction_type'].str.upper().str.strip()
            valid_types = ['EMI', 'PREPAYMENT', 'FORECLOSURE', 'DISBURSEMENT', 'PENALTY', 'FEE']
            df_clean.loc[~df_clean['transaction_type'].isin(valid_types), 'transaction_type'] = 'EMI'
        
        if 'transaction_mode' in df_clean.columns:
            df_clean['transaction_mode'] = df_clean['transaction_mode'].str.upper().str.strip()
        
        if 'transaction_status' in df_clean.columns and 'reconciliation_status' in df_clean.columns:
            df_clean.loc[df_clean['transaction_status'] == 'Failed', 'reconciliation_status'] = 'Unmatched'
        
        if 'transaction_date' in df_clean.columns:
            df_clean = df_clean[df_clean['transaction_date'] <= datetime.now()]
        
        logger.info(f"Transaction cleaning complete. Shape: {df_clean.shape}")
        return df_clean
    
    @staticmethod
    def _get_dpd_bucket_consistent(dpd):
        if pd.isna(dpd) or dpd == 0:
            return '0'
        elif dpd <= 30:
            return '1-30'
        elif dpd <= 60:
            return '31-60'
        elif dpd <= 90:
            return '61-90'
        else:
            return '90+'


class DataQualityAnalyzer:
    @staticmethod
    def validate_customers(df: pd.DataFrame) -> Dict:
        logger.info("Validating customer data for analytics...")
        
        checks = {
            'total_records': len(df),
            'missing_values': df.isnull().sum().to_dict() if not df.empty else {},
            'duplicate_customer_ids': df['customer_id'].duplicated().sum() if 'customer_id' in df.columns else 0,
            'invalid_credit_scores': ((df['credit_score'] < 300) | (df['credit_score'] > 900)).sum() if 'credit_score' in df.columns else 0,
            'negative_income': (df['annual_income'] < 0).sum() if 'annual_income' in df.columns else 0,
            'invalid_age': ((df['age'] < 18) | (df['age'] > 100)).sum() if 'age' in df.columns else 0,
        }
        
        if len(df) > 0 and len(df.columns) > 0:
            checks['pass_rate'] = 1 - (sum(v for k, v in checks.items() if isinstance(v, (int, float)) and k != 'missing_values') / 
                                      (len(df) * len(df.columns)))
        else:
            checks['pass_rate'] = 0
        
        logger.info("Customer Data Quality Report (Analytics):")
        for check, value in checks.items():
            if check != 'missing_values':
                logger.info(f"  {check}: {value}")
        
        return checks
    
    @staticmethod
    def validate_loans(df: pd.DataFrame) -> Dict:
        checks = {
            'total_records': len(df),
            'missing_customer_id': df['customer_id'].isnull().sum() if 'customer_id' in df.columns else 0,
            'zero_loan_amount': (df['loan_amount'] <= 0).sum() if 'loan_amount' in df.columns else 0,
            'invalid_interest': ((df['interest_rate'] < 5) | (df['interest_rate'] > 30)).sum() if 'interest_rate' in df.columns else 0,
            'negative_tenure': (df['tenure_months'] <= 0).sum() if 'tenure_months' in df.columns else 0,
            'future_dates': 0
        }
        
        if 'application_date' in df.columns:
            future_app = (pd.to_datetime(df['application_date']) > datetime.now()).sum()
            checks['future_dates'] += future_app
        if 'disbursement_date' in df.columns:
            future_dis = (pd.to_datetime(df['disbursement_date']) > datetime.now()).sum()
            checks['future_dates'] += future_dis
        
        logger.info("Loan Data Quality Report (Analytics):")
        for check, value in checks.items():
            logger.info(f"  {check}: {value}")
        
        return checks


class FeatureEngineer:
    @staticmethod
    def create_customer_features(df: pd.DataFrame) -> pd.DataFrame:
        df_feat = df.copy()
        
        if df_feat.empty:
            return df_feat
        
        # 1. Age groups
        if 'age' in df_feat.columns:
            df_feat['age_group'] = pd.cut(
                df_feat['age'],
                bins=[0, 25, 35, 50, 100],
                labels=['Young', 'Mid-Career', 'Senior', 'Retired'],
                include_lowest=True
            )
        
        # 2. Income to credit score ratio
        if 'annual_income' in df_feat.columns and 'credit_score' in df_feat.columns:
            df_feat['income_credit_ratio'] = df_feat['annual_income'] / (df_feat['credit_score'] + 1)
        
        # 3. Employment stability (proxy)
        if 'employment_type' in df_feat.columns:
            df_feat['is_stable_employment'] = df_feat['employment_type'].isin([
                'Salaried', 'Government', 'Public Sector'
            ]).astype(int)
        
        # 4. Location tier
        if 'city' in df_feat.columns:
            metro_cities = ['Mumbai', 'Delhi', 'Bangalore', 'Chennai', 'Kolkata', 'Hyderabad', 'Pune', 'Ahmedabad']
            df_feat['is_metro'] = df_feat['city'].isin(metro_cities).astype(int)
        
        return df_feat
    
    @staticmethod
    def create_loan_features(df: pd.DataFrame) -> pd.DataFrame:
        df_feat = df.copy()
        
        if df_feat.empty:
            return df_feat
        
        # 1. Debt to income ratio (estimated)
        if 'emi_amount' in df_feat.columns and 'annual_income' in df_feat.columns:
            df_feat['estimated_dti'] = df_feat['emi_amount'] * 12 / (df_feat['annual_income'] + 1)
            df_feat['estimated_dti'] = df_feat['estimated_dti'].clip(0, 1)
        
        # 2. Interest rate spread
        if 'interest_rate' in df_feat.columns:
            df_feat['interest_rate_spread'] = df_feat['interest_rate'] - 8  # RBI repo + spread
        
        # 3. Loan to value ratio fix
        if 'loan_to_value_ratio' in df_feat.columns and 'loan_amount' in df_feat.columns and 'collateral_value' in df_feat.columns:
            df_feat['ltv_ratio_clean'] = df_feat.apply(
                lambda x: x['loan_to_value_ratio'] if pd.notna(x['loan_to_value_ratio']) 
                else x['loan_amount'] / (x['collateral_value'] + 1) * 100,
                axis=1
            )
        
        # 4. Repayment ratio
        if 'current_balance' in df_feat.columns and 'loan_amount' in df_feat.columns:
            df_feat['repayment_ratio'] = 1 - (df_feat['current_balance'] / (df_feat['loan_amount'] + 1))
        
        # 5. Risk score composite
        if all(col in df_feat.columns for col in ['probability_of_default', 'bureau_score_at_origination', 'days_past_due']):
            df_feat['composite_risk_score'] = (
                df_feat['probability_of_default'].fillna(0) * 0.4 +
                (1 - df_feat['bureau_score_at_origination'].fillna(650) / 900) * 0.3 +
                (df_feat['days_past_due'].fillna(0) / 180) * 0.3
            )
        
        return df_feat