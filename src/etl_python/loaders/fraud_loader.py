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

class FraudAlertFactLoader:
    def __init__(self, db_connection: DatabaseConnection):
        self.db = db_connection
        self.utils = ETLUtils()
        self.quality_checker = DataQualityChecker()
        self.config = self.utils.load_config()
        
        self.loan_cache = {}
        self.customer_cache = {}
        self.transaction_cache = {}
        self.date_cache = set()
    
    def extract(self, file_path: str = 'data/raw_csv/fraud_alerts.csv') -> pd.DataFrame:
        logger.info("📤 Extracting fraud alert data...")
        
        try:
            df = pd.read_csv(file_path)
            logger.info(f"✅ Extracted {len(df)} fraud alert records from {file_path}")
            return df
        except FileNotFoundError:
            logger.warning(f"⚠️ Fraud alert file not found: {file_path}")
            return pd.DataFrame()
    
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
        
        transactions_df = self.db.query_to_dataframe(
            "SELECT transaction_sk, transaction_id FROM fact_transaction"
        )
        self.transaction_cache = dict(zip(transactions_df['transaction_id'], transactions_df['transaction_sk']))
        logger.info(f"✅ Loaded {len(self.transaction_cache)} transaction keys")
        
        dates_df = self.db.query_to_dataframe("SELECT date_sk FROM dim_date")
        self.date_cache = set(dates_df['date_sk'].tolist())
        logger.info(f"✅ Loaded {len(self.date_cache)} date keys")

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("🔄 Transforming fraud alert data...")
        
        if df.empty:
            logger.warning("⚠️ No fraud alerts to transform")
            return df
        
        df_transformed = df.copy()
        
        numeric_columns = ['risk_score', 'financial_impact']
        for col in numeric_columns:
            if col in df_transformed.columns:
                df_transformed[col] = pd.to_numeric(df_transformed[col], errors='coerce')
        
        date_columns = ['detection_date', 'resolution_date']
        for col in date_columns:
            if col in df_transformed.columns:
                df_transformed[col] = pd.to_datetime(df_transformed[col], errors='coerce')
        
        logger.info("  🔑 Adding dimension surrogate keys...")
        
        df_transformed['loan_sk'] = df_transformed['loan_id'].map(self.loan_cache)
        df_transformed['customer_sk'] = df_transformed['customer_id'].map(self.customer_cache)
        df_transformed['transaction_sk'] = df_transformed['transaction_id'].map(self.transaction_cache)
        df_transformed['detection_date_sk'] = df_transformed['detection_date'].apply(self.utils.date_to_sk)
        
        df_transformed['created_at'] = datetime.now()
        df_transformed['updated_at'] = datetime.now()
        
        df_transformed['risk_level'] = df_transformed['risk_level'].fillna('Medium')
        df_transformed['investigation_status'] = df_transformed['investigation_status'].fillna('New')
        df_transformed['alert_category'] = df_transformed['alert_category'].fillna('Application Fraud')
        
        initial_count = len(df_transformed)
        df_transformed = df_transformed.dropna(subset=['customer_sk', 'detection_date_sk'])
        dropped_count = initial_count - len(df_transformed)
        
        if dropped_count > 0:
            logger.warning(f"⚠️  Dropped {dropped_count} records with missing dimension keys")
        
        logger.info(f"✅ Transformed {len(df_transformed)} fraud alert records")
        return df_transformed
    
    def load(self, df: pd.DataFrame) -> int:
        logger.info("📥 Loading fraud alert data...")
        
        if df.empty:
            logger.warning("⚠️ No fraud alerts to load")
            return 0
        
        critical_columns = ['alert_id', 'customer_sk', 'detection_date_sk']
        quality_results = self.quality_checker.check_completeness(df, critical_columns)
        
        for col, result in quality_results.items():
            if not result['passed']:
                logger.warning(f"⚠️  Column {col} has {result['null_percentage']}% nulls")
        
        duplicate_check = self.quality_checker.check_uniqueness(df, ['alert_id'])
        if not duplicate_check['alert_id']['passed']:
            logger.warning(f"⚠️  Found {duplicate_check['alert_id']['duplicate_count']} duplicate alert IDs")
            df = df.drop_duplicates(subset=['alert_id'], keep='last')
            logger.info(f"✅ Removed duplicates, {len(df)} records remaining")
        
        logger.info("📦 Loading fraud alert records...")
        
        batch_size = 5000
        batches = self.utils.get_batch_ranges(len(df), batch_size)
        
        total_loaded = 0
        for i, (start_idx, end_idx) in enumerate(batches):
            batch_df = df.iloc[start_idx:end_idx]
            
            batch_df = batch_df.where(pd.notnull(batch_df), None)
            
            for _, row in batch_df.iterrows():
                query = """
                INSERT INTO fact_fraud_alert (
                    alert_id, loan_sk, customer_sk, transaction_sk, detection_date_sk,
                    alert_type, alert_category, risk_score, risk_level, detection_method,
                    rule_triggered, alert_description, assigned_to, investigation_status,
                    investigation_notes, resolution_date, financial_impact,
                    created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                values = (
                    None if pd.isna(row.get('alert_id')) else row['alert_id'],
                    None if pd.isna(row.get('loan_sk')) else row['loan_sk'],
                    None if pd.isna(row.get('customer_sk')) else row['customer_sk'],
                    None if pd.isna(row.get('transaction_sk')) else row['transaction_sk'],
                    None if pd.isna(row.get('detection_date_sk')) else row['detection_date_sk'],
                    None if pd.isna(row.get('alert_type')) else row['alert_type'],
                    None if pd.isna(row.get('alert_category')) else row['alert_category'],
                    None if pd.isna(row.get('risk_score')) else row['risk_score'],
                    None if pd.isna(row.get('risk_level')) else row['risk_level'],
                    None if pd.isna(row.get('detection_method')) else row['detection_method'],
                    None if pd.isna(row.get('rule_triggered')) else row['rule_triggered'],
                    None if pd.isna(row.get('alert_description')) else row['alert_description'],
                    None if pd.isna(row.get('assigned_to')) else row['assigned_to'],
                    None if pd.isna(row.get('investigation_status')) else row['investigation_status'],
                    None if pd.isna(row.get('investigation_notes')) else row['investigation_notes'],
                    None if pd.isna(row.get('resolution_date')) else row['resolution_date'],
                    None if pd.isna(row.get('financial_impact')) else row['financial_impact'],
                    None if pd.isna(row.get('created_at')) else row['created_at'],
                    None if pd.isna(row.get('updated_at')) else row['updated_at']
                )
                
                try:
                    self.db.execute_query(query, values)
                except Exception as e:
                    logger.error(f"❌ Error inserting row: {e}")
                    logger.error(f"Row data: {values}")
                    raise
            
            total_loaded += len(batch_df)
            logger.info(f"  📦 Batch {i+1}/{len(batches)}: Loaded {len(batch_df)} records")
        
        logger.info(f"✅ Loaded {total_loaded} fraud alert records")
        
        self.utils.create_etl_control_record(
            self.db, 
            "FRAUD_ALERT_FACT_LOAD", 
            "fact_fraud_alert", 
            "SUCCESS", 
            total_loaded
        )
        
        return total_loaded
    
    def run_pipeline(self, file_path: str = 'data/raw_csv/fraud_alerts.csv') -> Dict:
        logger.info("=" * 60)
        logger.info("🚀 FRAUD ALERT FACT ETL PIPELINE")
        logger.info("=" * 60)
        
        try:
            self.load_dimension_caches()
            
            df = self.extract(file_path)
            
            if df.empty:
                logger.warning("⚠️ No fraud alerts to process")
                return {
                    'status': 'SKIPPED',
                    'records_extracted': 0,
                    'records_transformed': 0,
                    'records_loaded': 0,
                    'quality_score': 100
                }
            
            df_transformed = self.transform(df)
            
            quality_report = self.quality_checker.generate_quality_report(
                df_transformed, 'fact_fraud_alert'
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
            logger.info(f"✅ FRAUD ALERT ETL COMPLETE: {records_loaded} records loaded")
            logger.info("=" * 60)
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Fraud alert ETL failed: {e}")
            self.utils.create_etl_control_record(
                self.db, 
                "FRAUD_ALERT_FACT_LOAD", 
                "fact_fraud_alert", 
                "FAILED", 
                0, 
                str(e)
            )
            raise


if __name__ == "__main__":
    db = DatabaseConnection()
    loader = FraudAlertFactLoader(db)
    result = loader.run_pipeline()
    print(f"✅ Loaded {result['records_loaded']} fraud alerts")