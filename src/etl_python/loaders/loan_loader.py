import pandas as pd
import numpy as np
from datetime import datetime
import logging
from typing import Dict, Tuple, Optional
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from src.database.db_connection import DatabaseConnection
from src.etl_python.etl_utils import ETLUtils, DataQualityChecker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LoanFactLoader:
    def __init__(self, db_connection: DatabaseConnection):
        self.db = db_connection
        self.utils = ETLUtils()
        self.quality_checker = DataQualityChecker()
        self.config = self.utils.load_config()
        
        # Cache for dimension lookups
        self.customer_cache = {}
        self.product_cache = {}
        self.branch_cache = {}
        self.date_cache = set()
    
    def extract(self, file_path: str = 'data/raw_csv/loans.csv') -> pd.DataFrame:
        logger.info("📤 Extracting loan data...")
        
        try:
            df = pd.read_csv(file_path)
            logger.info(f"✅ Extracted {len(df)} loan records from {file_path}")
            return df
        except FileNotFoundError:
            logger.error(f"❌ Loan file not found: {file_path}")
            raise
    
    def load_dimension_caches(self):
        logger.info("🔄 Loading dimension caches...")
        
        # Load customers
        customers_df = self.db.query_to_dataframe(
            "SELECT customer_sk, customer_id FROM dim_customer WHERE is_current = 1"
        )
        self.customer_cache = dict(zip(customers_df['customer_id'], customers_df['customer_sk']))
        logger.info(f"✅ Loaded {len(self.customer_cache)} customer keys")
        
        # Load products
        products_df = self.db.query_to_dataframe(
            "SELECT product_sk, product_id FROM dim_product WHERE is_active = 1"
        )
        self.product_cache = dict(zip(products_df['product_id'], products_df['product_sk']))
        logger.info(f"✅ Loaded {len(self.product_cache)} product keys")
        
        # Load branches
        branches_df = self.db.query_to_dataframe(
            "SELECT branch_sk, branch_id FROM dim_branch WHERE is_active = 1"
        )
        self.branch_cache = dict(zip(branches_df['branch_id'], branches_df['branch_sk']))
        logger.info(f"✅ Loaded {len(self.branch_cache)} branch keys")
        
        # Load dates
        dates_df = self.db.query_to_dataframe("SELECT date_sk FROM dim_date")
        self.date_cache = set(dates_df['date_sk'].tolist())
        logger.info(f"✅ Loaded {len(self.date_cache)} date keys")

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("🔄 Transforming loan data...")
        
        df_transformed = df.copy()
        
        numeric_columns = ['loan_amount', 'sanctioned_amount', 'interest_rate', 
                          'tenure_months', 'emi_amount', 'processing_fee', 
                          'gst_on_fee', 'net_disbursed_amount', 'collateral_value',
                          'loan_to_value_ratio', 'co_applicant_income',
                          'bureau_score_at_origination', 'probability_of_default',
                          'loss_given_default', 'exposure_at_default', 'expected_loss',
                          'current_balance', 'overdue_amount', 'days_past_due',
                          'written_off_amount', 'foreclosure_amount']
        
        for col in numeric_columns:
            if col in df_transformed.columns:
                df_transformed[col] = pd.to_numeric(df_transformed[col], errors='coerce')
        
        date_columns = ['application_date', 'disbursement_date', 'first_emi_date',
                       'npa_date', 'restructuring_date', 'written_off_date',
                       'foreclosure_date', 'fraud_detection_date']
        
        for col in date_columns:
            if col in df_transformed.columns:
                df_transformed[col] = pd.to_datetime(df_transformed[col], errors='coerce')
        
        initial_count = len(df_transformed)
        df_transformed = df_transformed[df_transformed['disbursement_date'].notna()]
        rejected_count = initial_count - len(df_transformed)
        if rejected_count > 0:
            logger.info(f"📉 Filtered out {rejected_count} rejected/non-disbursed loans")
        
        logger.info("  🔑 Adding dimension surrogate keys...")
        
        df_transformed['customer_sk'] = df_transformed['customer_id'].map(self.customer_cache)
        df_transformed['product_sk'] = df_transformed['product_id'].map(self.product_cache)
        df_transformed['branch_sk'] = df_transformed['branch_id'].map(self.branch_cache)
        df_transformed['application_date_sk'] = df_transformed['application_date'].apply(self.utils.date_to_sk)
        df_transformed['disbursement_date_sk'] = df_transformed['disbursement_date'].apply(self.utils.date_to_sk)
        df_transformed['first_emi_date_sk'] = df_transformed['first_emi_date'].apply(self.utils.date_to_sk)
        
        df_transformed['dpd_bucket'] = df_transformed['dpd_bucket'].fillna('0')
        df_transformed['loan_status'] = df_transformed['loan_status'].fillna('Active')
        df_transformed['collection_tier'] = df_transformed['collection_tier'].fillna(1)
        df_transformed['npa_flag'] = df_transformed['npa_flag'].fillna(False)
        df_transformed['fraud_flag'] = df_transformed['fraud_flag'].fillna(False)

        df_transformed['created_at'] = datetime.now()
        df_transformed['updated_at'] = datetime.now()
        
        initial_count = len(df_transformed)
        df_transformed = df_transformed.dropna(subset=['customer_sk', 'product_sk', 'branch_sk', 'application_date_sk'])
        dropped_count = initial_count - len(df_transformed)
        
        if dropped_count > 0:
            logger.warning(f"⚠️  Dropped {dropped_count} records with missing dimension keys")
        
        logger.info(f"✅ Transformed {len(df_transformed)} loan records (disbursed loans)")
        return df_transformed
    

    def load(self, df: pd.DataFrame) -> int:
        logger.info("📥 Loading loan data...")
        
        critical_columns = ['loan_id', 'customer_sk', 'product_sk', 'branch_sk', 'application_date_sk']
        quality_results = self.quality_checker.check_completeness(df, critical_columns)
        
        for col, result in quality_results.items():
            if not result['passed']:
                logger.warning(f"⚠️  Column {col} has {result['null_percentage']}% nulls")
        
        duplicate_check = self.quality_checker.check_uniqueness(df, ['loan_id'])
        if not duplicate_check['loan_id']['passed']:
            logger.warning(f"⚠️  Found {duplicate_check['loan_id']['duplicate_count']} duplicate loan IDs")
            df = df.drop_duplicates(subset=['loan_id'], keep='last')
            logger.info(f"✅ Removed duplicates, {len(df)} records remaining")
        
        logger.info("📦 Loading loan records...")
        
        batch_size = 5000
        batches = self.utils.get_batch_ranges(len(df), batch_size)
        
        total_loaded = 0
        for i, (start_idx, end_idx) in enumerate(batches):
            batch_df = df.iloc[start_idx:end_idx]
            
            batch_df = batch_df.where(pd.notnull(batch_df), None)
            
            for _, row in batch_df.iterrows():
                query = """
                INSERT INTO fact_loan (
                    loan_id, customer_sk, product_sk, branch_sk, application_date_sk,
                    disbursement_date_sk, first_emi_date_sk, loan_amount, sanctioned_amount,
                    interest_rate, tenure_months, emi_amount, processing_fee, gst_on_fee,
                    net_disbursed_amount, loan_purpose, collateral_id, collateral_value,
                    loan_to_value_ratio, co_applicant_present, co_applicant_income,
                    bureau_score_at_origination, internal_risk_rating, probability_of_default,
                    loss_given_default, exposure_at_default, expected_loss, current_balance,
                    overdue_amount, days_past_due, dpd_bucket, npa_flag, npa_date,
                    restructuring_flag, restructuring_date, written_off_flag, written_off_date,
                    written_off_amount, loan_status, foreclosure_date, foreclosure_amount,
                    fraud_flag, fraud_type, fraud_detection_date, collection_tier,
                    assigned_collection_agent, created_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s
                )
                """

                def clean_value(val):
                    return None if pd.isna(val) else val

                values = (
                    clean_value(row.get('loan_id')),
                    clean_value(row.get('customer_sk')),
                    clean_value(row.get('product_sk')),
                    clean_value(row.get('branch_sk')),
                    clean_value(row.get('application_date_sk')),
                    clean_value(row.get('disbursement_date_sk')),
                    clean_value(row.get('first_emi_date_sk')),
                    clean_value(row.get('loan_amount')),
                    clean_value(row.get('sanctioned_amount')),
                    clean_value(row.get('interest_rate')),
                    clean_value(row.get('tenure_months')),
                    clean_value(row.get('emi_amount')),
                    clean_value(row.get('processing_fee')),
                    clean_value(row.get('gst_on_fee')),
                    clean_value(row.get('net_disbursed_amount')),
                    clean_value(row.get('loan_purpose')),
                    clean_value(row.get('collateral_id')),
                    clean_value(row.get('collateral_value')),
                    clean_value(row.get('loan_to_value_ratio')),
                    clean_value(row.get('co_applicant_present')),
                    clean_value(row.get('co_applicant_income')),
                    clean_value(row.get('bureau_score_at_origination')),
                    clean_value(row.get('internal_risk_rating')),
                    clean_value(row.get('probability_of_default')),
                    clean_value(row.get('loss_given_default')),
                    clean_value(row.get('exposure_at_default')),
                    clean_value(row.get('expected_loss')),
                    clean_value(row.get('current_balance')),
                    clean_value(row.get('overdue_amount')),
                    clean_value(row.get('days_past_due')),
                    clean_value(row.get('dpd_bucket')),
                    clean_value(row.get('npa_flag')),
                    clean_value(row.get('npa_date')),
                    clean_value(row.get('restructuring_flag')),
                    clean_value(row.get('restructuring_date')),
                    clean_value(row.get('written_off_flag')),
                    clean_value(row.get('written_off_date')),
                    clean_value(row.get('written_off_amount')),
                    clean_value(row.get('loan_status')),
                    clean_value(row.get('foreclosure_date')),
                    clean_value(row.get('foreclosure_amount')),
                    clean_value(row.get('fraud_flag')),
                    clean_value(row.get('fraud_type')),
                    clean_value(row.get('fraud_detection_date')),
                    clean_value(row.get('collection_tier')),
                    clean_value(row.get('assigned_collection_agent')),
                    clean_value(row.get('created_at'))
                )
                

                if len(values) != 47:
                    logger.error(f"❌ VALUES COUNT MISMATCH: {len(values)} values (should be 47)")
                    logger.error(f"Columns in values tuple: {[col for col in values if col is not None][:10]}...")
                    raise ValueError(f"Expected 47 values, got {len(values)}")
                
                self.db.execute_query(query, values)
            
            total_loaded += len(batch_df)
            logger.info(f"  📦 Batch {i+1}/{len(batches)}: Loaded {len(batch_df)} records")
        
        logger.info(f"✅ Loaded {total_loaded} loan records")
        
        self.utils.create_etl_control_record(
            self.db, 
            "LOAN_FACT_LOAD", 
            "fact_loan", 
            "SUCCESS", 
            total_loaded
        )
        
        return total_loaded
    
    def run_pipeline(self, file_path: str = 'data/raw_csv/loans.csv') -> Dict:
        """Run complete ETL pipeline for loans"""
        logger.info("=" * 60)
        logger.info("🚀 LOAN FACT ETL PIPELINE")
        logger.info("=" * 60)
        
        try:
            self.load_dimension_caches()
            
            df = self.extract(file_path)
            
            df_transformed = self.transform(df)
            
            quality_report = self.quality_checker.generate_quality_report(
                df_transformed, 'fact_loan'
            )
            logger.info(f"📊 Data Quality Score: {quality_report['quality_score']}%")
            
            records_loaded = self.load(df_transformed)
            
            result = {
                'status': 'SUCCESS',
                'records_extracted': len(df),
                'records_transformed': len(df_transformed),
                'records_loaded': records_loaded,
                'quality_score': quality_report['quality_score']
            }
            
            logger.info("=" * 60)
            logger.info(f"✅ LOAN ETL COMPLETE: {records_loaded} records loaded")
            logger.info("=" * 60)
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Loan ETL failed: {e}")
            self.utils.create_etl_control_record(
                self.db, 
                "LOAN_FACT_LOAD", 
                "fact_loan", 
                "FAILED", 
                0, 
                str(e)
            )
            raise


if __name__ == "__main__":
    db = DatabaseConnection()
    loader = LoanFactLoader(db)
    result = loader.run_pipeline()
    print(f"✅ Loaded {result['records_loaded']} loans")