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

class TransactionFactLoader:
    def __init__(self, db_connection: DatabaseConnection):
        self.db = db_connection
        self.utils = ETLUtils()
        self.quality_checker = DataQualityChecker()
        self.config = self.utils.load_config()
        
        self.loan_cache = {}
        self.customer_cache = {}
        self.date_cache = set()
    
    def extract(self, file_path: str = 'data/raw_csv/transactions.csv') -> pd.DataFrame:
        logger.info("📤 Extracting transaction data...")
        
        try:
            df = pd.read_csv(file_path)
            logger.info(f"✅ Extracted {len(df)} transaction records from {file_path}")
            return df
        except FileNotFoundError:
            logger.error(f"❌ Transaction file not found: {file_path}")
            raise
    
    def load_dimension_caches(self):
        logger.info("🔄 Loading dimension caches...")
        
        loans_df = self.db.query_to_dataframe(
            "SELECT loan_sk, loan_id FROM fact_loan"
        )
        self.loan_cache = dict(zip(loans_df['loan_id'], loans_df['loan_sk']))
        logger.info(f"✅ Loaded {len(self.loan_cache)} loan keys")
        
        customers_df = self.db.query_to_dataframe(
            "SELECT customer_sk, customer_id FROM dim_customer WHERE is_current = 1"
        )
        self.customer_cache = dict(zip(customers_df['customer_id'], customers_df['customer_sk']))
        logger.info(f"✅ Loaded {len(self.customer_cache)} customer keys")
        
        dates_df = self.db.query_to_dataframe("SELECT date_sk FROM dim_date")
        self.date_cache = set(dates_df['date_sk'].tolist())
        logger.info(f"✅ Loaded {len(self.date_cache)} date keys")
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("🔄 Transforming transaction data...")
        
        if df.empty:
            logger.warning("⚠️ No transactions to transform")
            return df
        
        df_transformed = df.copy()
        
        numeric_columns = ['amount', 'principal_component', 'interest_component',
                          'penalty_component', 'gst_component']
        
        for col in numeric_columns:
            if col in df_transformed.columns:
                df_transformed[col] = pd.to_numeric(df_transformed[col], errors='coerce')
        
        date_columns = ['transaction_date', 'reconciled_date']
        for col in date_columns:
            if col in df_transformed.columns:
                df_transformed[col] = pd.to_datetime(df_transformed[col], errors='coerce')
        
        logger.info("  🔑 Adding dimension surrogate keys...")
        
        df_transformed['loan_sk'] = df_transformed['loan_id'].map(self.loan_cache)
        df_transformed['customer_sk'] = df_transformed['customer_id'].map(self.customer_cache)
        df_transformed['transaction_date_sk'] = df_transformed['transaction_date'].apply(self.utils.date_to_sk)
        
        df_transformed['created_at'] = datetime.now()
        df_transformed['updated_at'] = datetime.now()
        
        df_transformed['transaction_status'] = df_transformed['transaction_status'].fillna('Success')
        df_transformed['reconciliation_status'] = df_transformed['reconciliation_status'].fillna('Pending')
        df_transformed['transaction_type'] = df_transformed['transaction_type'].fillna('EMI')
        df_transformed['transaction_mode'] = df_transformed['transaction_mode'].fillna('NEFT')
        
        initial_count = len(df_transformed)
        df_transformed = df_transformed.dropna(subset=['loan_sk', 'customer_sk', 'transaction_date_sk'])
        dropped_count = initial_count - len(df_transformed)
        
        if dropped_count > 0:
            logger.warning(f"⚠️  Dropped {dropped_count} records with missing dimension keys")
        
        logger.info(f"✅ Transformed {len(df_transformed)} transaction records")
        return df_transformed
    
    def load(self, df: pd.DataFrame) -> int:
        logger.info("📥 Loading transaction data...")
        
        if df.empty:
            logger.warning("⚠️ No transactions to load")
            return 0
        
        critical_columns = ['transaction_id', 'loan_sk', 'customer_sk', 'transaction_date_sk']
        quality_results = self.quality_checker.check_completeness(df, critical_columns)
        
        for col, result in quality_results.items():
            if not result['passed']:
                logger.warning(f"⚠️  Column {col} has {result['null_percentage']}% nulls")
        
        duplicate_check = self.quality_checker.check_uniqueness(df, ['transaction_id'])
        if not duplicate_check['transaction_id']['passed']:
            logger.warning(f"⚠️  Found {duplicate_check['transaction_id']['duplicate_count']} duplicate transaction IDs")
            df = df.drop_duplicates(subset=['transaction_id'], keep='last')
            logger.info(f"✅ Removed duplicates, {len(df)} records remaining")
        
        logger.info("📦 Loading transaction records...")
        
        batch_size = 5000
        batches = self.utils.get_batch_ranges(len(df), batch_size)
        
        total_loaded = 0
        for i, (start_idx, end_idx) in enumerate(batches):
            batch_df = df.iloc[start_idx:end_idx]
            
            batch_df = batch_df.where(pd.notnull(batch_df), None)
            
            for _, row in batch_df.iterrows():
                query = """
                INSERT INTO fact_transaction (
                    transaction_id, loan_sk, customer_sk, transaction_date_sk,
                    transaction_type, transaction_mode, amount, principal_component,
                    interest_component, penalty_component, gst_component,
                    payment_reference, bank_name, bank_account_last4,
                    transaction_status, failure_reason, reconciliation_status,
                    reconciled_date, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                def clean_value(val):
                    return None if pd.isna(val) else val
                
                values = (
                    clean_value(row.get('transaction_id')),
                    clean_value(row.get('loan_sk')),
                    clean_value(row.get('customer_sk')),
                    clean_value(row.get('transaction_date_sk')),
                    clean_value(row.get('transaction_type')),
                    clean_value(row.get('transaction_mode')),
                    clean_value(row.get('amount')),
                    clean_value(row.get('principal_component')),
                    clean_value(row.get('interest_component')),
                    clean_value(row.get('penalty_component')),
                    clean_value(row.get('gst_component')),
                    clean_value(row.get('payment_reference')),
                    clean_value(row.get('bank_name')),
                    clean_value(row.get('bank_account_last4')),
                    clean_value(row.get('transaction_status')),
                    clean_value(row.get('failure_reason')),
                    clean_value(row.get('reconciliation_status')),
                    clean_value(row.get('reconciled_date')),
                    clean_value(row.get('created_at'))
                )
                
                if len(values) != 19:
                    logger.error(f"❌ VALUES COUNT MISMATCH: {len(values)} values (should be 19)")
                
                self.db.execute_query(query, values)
            
            total_loaded += len(batch_df)
            logger.info(f"  📦 Batch {i+1}/{len(batches)}: Loaded {len(batch_df)} records")
        
        logger.info(f"✅ Loaded {total_loaded} transaction records")
        
        self.utils.create_etl_control_record(
            self.db, 
            "TRANSACTION_FACT_LOAD", 
            "fact_transaction", 
            "SUCCESS", 
            total_loaded
        )
        
        return total_loaded
    
    def run_pipeline(self, file_path: str = 'data/raw_csv/transactions.csv') -> Dict:
        logger.info("=" * 60)
        logger.info("🚀 TRANSACTION FACT ETL PIPELINE")
        logger.info("=" * 60)
        
        try:
            self.load_dimension_caches()
            
            df = self.extract(file_path)
            
            df_transformed = self.transform(df)
            
            quality_report = self.quality_checker.generate_quality_report(
                df_transformed, 'fact_transaction'
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
            logger.info(f"✅ TRANSACTION ETL COMPLETE: {records_loaded} records loaded")
            logger.info("=" * 60)
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Transaction ETL failed: {e}")
            self.utils.create_etl_control_record(
                self.db, 
                "TRANSACTION_FACT_LOAD", 
                "fact_transaction", 
                "FAILED", 
                0, 
                str(e)
            )
            raise


if __name__ == "__main__":
    db = DatabaseConnection()
    loader = TransactionFactLoader(db)
    result = loader.run_pipeline()
    print(f"✅ Loaded {result['records_loaded']} transactions")