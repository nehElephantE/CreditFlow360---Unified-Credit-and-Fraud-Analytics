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

class CustomerDimensionLoader:
    def __init__(self, db_connection: DatabaseConnection):
        self.db = db_connection
        self.utils = ETLUtils()
        self.quality_checker = DataQualityChecker()
        self.config = self.utils.load_config()
    
    def extract(self, file_path: str = 'data/raw_csv/customers.csv') -> pd.DataFrame:
        logger.info("📤 Extracting customer data...")
        
        try:
            df = pd.read_csv(file_path)
            logger.info(f"✅ Extracted {len(df)} customer records from {file_path}")
            return df
        except FileNotFoundError:
            logger.error(f"❌ Customer file not found: {file_path}")
            raise
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("🔄 Transforming customer data...")
        
        df_transformed = df.copy()
        
        string_columns = ['first_name', 'last_name', 'gender', 'marital_status', 
                         'education', 'employment_type', 'city', 'state', 
                         'pincode', 'phone', 'email', 'customer_segment', 
                         'customer_value_tier', 'acquisition_channel']
        
        for col in string_columns:
            if col in df_transformed.columns:
                df_transformed[col] = df_transformed[col].apply(self.utils.clean_string)
        
        numeric_columns = ['annual_income', 'credit_score', 'age']
        for col in numeric_columns:
            if col in df_transformed.columns:
                df_transformed[col] = pd.to_numeric(df_transformed[col], errors='coerce')
        
        date_columns = ['date_of_birth', 'acquisition_date', 'effective_start_date']
        for col in date_columns:
            if col in df_transformed.columns:
                df_transformed[col] = pd.to_datetime(df_transformed[col], errors='coerce')
        
        df_transformed['address_line2'] = df_transformed['address_line2'].fillna('')
        df_transformed['effective_end_date'] = df_transformed['effective_end_date'].fillna(pd.NaT)
        df_transformed['is_current'] = df_transformed['is_current'].fillna(1)
        df_transformed['is_active'] = df_transformed['is_active'].fillna(1)
        
        logger.info(f"✅ Transformed {len(df_transformed)} customer records")
        return df_transformed
    
    def load(self, df: pd.DataFrame) -> int:
        logger.info("📥 Loading customer data...")
        
        critical_columns = ['customer_id', 'first_name', 'last_name']
        quality_results = self.quality_checker.check_completeness(df, critical_columns)
        
        for col, result in quality_results.items():
            if not result['passed']:
                logger.warning(f"⚠️  Column {col} has {result['null_percentage']}% nulls")
        
        duplicate_check = self.quality_checker.check_uniqueness(df, ['customer_id'])
        if not duplicate_check['customer_id']['passed']:
            logger.warning(f"⚠️  Found {duplicate_check['customer_id']['duplicate_count']} duplicate customer IDs")
            df = df.drop_duplicates(subset=['customer_id'], keep='last')
            logger.info(f"✅ Removed duplicates, {len(df)} records remaining")
        

        logger.info("📦 Loading customer records...")


        batch_size = 5000
        batches = self.utils.get_batch_ranges(len(df), batch_size)
        
        total_loaded = 0
        for i, (start_idx, end_idx) in enumerate(batches):
            batch_df = df.iloc[start_idx:end_idx]
            
            batch_df = batch_df.where(pd.notnull(batch_df), None)
            
            for _, row in batch_df.iterrows():
                query = """
                INSERT INTO dim_customer (
                    customer_id, first_name, last_name, date_of_birth, age, gender,
                    marital_status, education, employment_type, annual_income, income_tier,
                    credit_score, credit_tier, city, state, pincode, address_line1,
                    address_line2, phone, email, customer_segment, customer_value_tier,
                    acquisition_date, acquisition_channel, is_active, effective_start_date,
                    effective_end_date, is_current
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                         %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                self.db.execute_query(query, tuple(row))
            
            total_loaded += len(batch_df)
            logger.info(f"  📦 Batch {i+1}/{len(batches)}: Loaded {len(batch_df)} records")
        
        logger.info(f"✅ Loaded {total_loaded} customer records")
        
        self.utils.create_etl_control_record(
            self.db, 
            "CUSTOMER_DIMENSION_LOAD", 
            "dim_customer", 
            "SUCCESS", 
            total_loaded
        )
        
        return total_loaded
    
    def run_pipeline(self, file_path: str = 'data/raw_csv/customers.csv') -> Dict:
        logger.info("=" * 60)
        logger.info("🚀 CUSTOMER DIMENSION ETL PIPELINE")
        logger.info("=" * 60)
        
        try:
            # Extract
            df = self.extract(file_path)
            
            # Transform
            df_transformed = self.transform(df)
            
            # Generate quality report
            quality_report = self.quality_checker.generate_quality_report(
                df_transformed, 'dim_customer'
            )
            logger.info(f"📊 Data Quality Score: {quality_report['quality_score']}%")
            
            # Load
            records_loaded = self.load(df_transformed)
            
            result = {
                'status': 'SUCCESS',
                'records_extracted': len(df),
                'records_transformed': len(df_transformed),
                'records_loaded': records_loaded,
                'quality_score': quality_report['quality_score']
            }
            
            logger.info("=" * 60)
            logger.info(f"✅ CUSTOMER ETL COMPLETE: {records_loaded} records loaded")
            logger.info("=" * 60)
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Customer ETL failed: {e}")
            self.utils.create_etl_control_record(
                self.db, 
                "CUSTOMER_DIMENSION_LOAD", 
                "dim_customer", 
                "FAILED", 
                0, 
                str(e)
            )
            raise


if __name__ == "__main__":
    db = DatabaseConnection()
    loader = CustomerDimensionLoader(db)
    result = loader.run_pipeline()
    print(f"✅ Loaded {result['records_loaded']} customers")