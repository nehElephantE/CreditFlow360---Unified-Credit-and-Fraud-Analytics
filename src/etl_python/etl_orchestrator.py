import time
import logging
from datetime import datetime
from typing import Dict, List
import pandas as pd
import json
import sys
import os
import numpy as np

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.database.db_connection import DatabaseConnection
from src.etl_python.etl_utils import ETLUtils, DataQualityChecker, CustomJSONEncoder
from src.etl_python.loaders.date_loader import DateDimensionLoader
from src.etl_python.loaders.customer_loader import CustomerDimensionLoader
from src.etl_python.loaders.loan_loader import LoanFactLoader
from src.etl_python.loaders.transaction_loader import TransactionFactLoader
from src.etl_python.loaders.fraud_loader import FraudAlertFactLoader

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/etl_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CreditFlowETL:
    def __init__(self):
        self.db = DatabaseConnection()
        self.utils = ETLUtils()
        self.config = self.utils.load_config()
        
        self.date_loader = DateDimensionLoader(self.db)
        self.customer_loader = CustomerDimensionLoader(self.db)
        self.loan_loader = LoanFactLoader(self.db)
        self.transaction_loader = TransactionFactLoader(self.db)
        self.fraud_loader = FraudAlertFactLoader(self.db)
        
        self.start_time = None
        self.end_time = None
        self.pipeline_results = {}
    
    def test_connections(self) -> bool:
        logger.info("🔌 Testing database connection...")
        
        result = self.db.test_connection()
        if result['status'] == 'success':
            logger.info(f"✅ Connected to MySQL: {result['version']}")
            logger.info(f"   Database: {result['database']}")
            logger.info(f"   Host: {result['host']}")
            return True
        else:
            logger.error(f"❌ Database connection failed: {result['message']}")
            return False
    
    def run_date_dimension_etl(self) -> Dict:
        logger.info("\n" + "=" * 60)
        logger.info("📅 STEP 1: DATE DIMENSION ETL")
        logger.info("=" * 60)
        
        try:
            records = self.date_loader.load_date_dimension()
            result = {
                'step': 'date_dimension',
                'status': 'SUCCESS',
                'records_loaded': records,
                'duration': 0
            }
            logger.info(f"✅ Date dimension ETL completed: {records} records")
            return result
        except Exception as e:
            logger.error(f"❌ Date dimension ETL failed: {e}")
            return {
                'step': 'date_dimension',
                'status': 'FAILED',
                'records_loaded': 0,
                'error': str(e)
            }
    
    def run_customer_dimension_etl(self) -> Dict:
        logger.info("\n" + "=" * 60)
        logger.info("👤 STEP 2: CUSTOMER DIMENSION ETL")
        logger.info("=" * 60)
        
        try:
            result = self.customer_loader.run_pipeline()
            result['step'] = 'customer_dimension'
            return result
        except Exception as e:
            logger.error(f"❌ Customer dimension ETL failed: {e}")
            return {
                'step': 'customer_dimension',
                'status': 'FAILED',
                'records_loaded': 0,
                'error': str(e)
            }
    
    def run_loan_fact_etl(self) -> Dict:
        logger.info("\n" + "=" * 60)
        logger.info("💰 STEP 3: LOAN FACT ETL")
        logger.info("=" * 60)
        
        try:
            result = self.loan_loader.run_pipeline()
            result['step'] = 'loan_fact'
            return result
        except Exception as e:
            logger.error(f"❌ Loan fact ETL failed: {e}")
            return {
                'step': 'loan_fact',
                'status': 'FAILED',
                'records_loaded': 0,
                'error': str(e)
            }
    
    def run_transaction_fact_etl(self) -> Dict:
        logger.info("\n" + "=" * 60)
        logger.info("💸 STEP 4: TRANSACTION FACT ETL")
        logger.info("=" * 60)
        
        try:
            result = self.transaction_loader.run_pipeline()
            result['step'] = 'transaction_fact'
            return result
        except Exception as e:
            logger.error(f"❌ Transaction fact ETL failed: {e}")
            return {
                'step': 'transaction_fact',
                'status': 'FAILED',
                'records_loaded': 0,
                'error': str(e)
            }
    
    def run_fraud_alert_fact_etl(self) -> Dict:
        logger.info("\n" + "=" * 60)
        logger.info("🚨 STEP 5: FRAUD ALERT FACT ETL")
        logger.info("=" * 60)
        
        try:
            result = self.fraud_loader.run_pipeline()
            result['step'] = 'fraud_alert_fact'
            return result
        except Exception as e:
            logger.error(f"❌ Fraud alert fact ETL failed: {e}")
            return {
                'step': 'fraud_alert_fact',
                'status': 'FAILED',
                'records_loaded': 0,
                'error': str(e)
            }
    
    def verify_load(self) -> Dict:
        logger.info("\n" + "=" * 60)
        logger.info("🔍 STEP 6: VERIFYING DATA LOAD")
        logger.info("=" * 60)
        
        verification = {}
        
        tables = [
            'dim_date',
            'dim_customer', 
            'dim_product',
            'dim_branch',
            'fact_loan',
            'fact_transaction',
            'fact_fraud_alert'
        ]
        
        for table in tables:
            try:
                result = self.db.query_to_dataframe(f"SELECT COUNT(*) as count FROM {table}")
                count = result['count'].iloc[0] if not result.empty else 0
                verification[table] = count
                logger.info(f"   {table}: {count:,} records")
            except Exception as e:
                verification[table] = 0
                logger.warning(f"   {table}: Error - {e}")
        
        logger.info("\n🔗 Checking referential integrity...")
        
        try:
            missing_customers = self.db.query_to_dataframe("""
                SELECT COUNT(*) as count 
                FROM fact_loan l 
                LEFT JOIN dim_customer c ON l.customer_sk = c.customer_sk 
                WHERE c.customer_sk IS NULL AND l.customer_sk IS NOT NULL
            """)
            verification['orphaned_loans'] = missing_customers['count'].iloc[0] if not missing_customers.empty else 0
        except:
            verification['orphaned_loans'] = 0
        
        try:
            missing_loans = self.db.query_to_dataframe("""
                SELECT COUNT(*) as count 
                FROM fact_transaction t 
                LEFT JOIN fact_loan l ON t.loan_sk = l.loan_sk 
                WHERE l.loan_sk IS NULL AND t.loan_sk IS NOT NULL
            """)
            verification['orphaned_transactions'] = missing_loans['count'].iloc[0] if not missing_loans.empty else 0
        except:
            verification['orphaned_transactions'] = 0
        
        logger.info(f"   Orphaned loans: {verification['orphaned_loans']}")
        logger.info(f"   Orphaned transactions: {verification['orphaned_transactions']}")
        
        return verification
    
    def generate_etl_report(self) -> Dict:
        
        report = {
            'etl_name': 'CreditFlow360_ETL',
            'etl_version': '1.0.0',
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration_seconds': (self.end_time - self.start_time).total_seconds() if self.end_time and self.start_time else 0,
            'pipeline_results': self.pipeline_results,
            'verification': self.verify_load(),
            'configuration': {
                'batch_size': 5000,
                'environment': 'production'
            }
        }
        
        failed_steps = [r for r in self.pipeline_results.values() if isinstance(r, dict) and r.get('status') == 'FAILED']
        report['overall_status'] = 'SUCCESS' if len(failed_steps) == 0 else 'PARTIAL_SUCCESS' if len(failed_steps) < len(self.pipeline_results) else 'FAILED'
        report['failed_steps_count'] = len(failed_steps)
        
        os.makedirs('data', exist_ok=True)
        report_path = f"data/etl_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2, default=str, cls=CustomJSONEncoder)
        
        logger.info(f"💾 ETL report saved to {report_path}")
        
        return report
    

    def run_all(self) -> Dict:
        self.start_time = datetime.now()
        logger.info("\n" + "=" * 60)
        logger.info("🚀 CREDITFLOW360 - COMPLETE ETL PIPELINE")
        logger.info("=" * 60)
        logger.info(f"Start Time: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 60 + "\n")
        
        if not self.test_connections():
            logger.error("❌ Cannot proceed with ETL - database connection failed")
            return {'status': 'FAILED', 'error': 'Database connection failed'}
        
        logger.info("🔓 Disabling foreign key checks...")
        self.db.execute_query("SET FOREIGN_KEY_CHECKS = 0")
        
        logger.info("\n" + "=" * 60)
        logger.info("🧹 STEP 1: CLEARING ALL TABLES")
        logger.info("=" * 60)
        
        tables_to_clear = [
            'fact_fraud_alert',      # Child tables first
            'fact_transaction',
            'fact_loan',
            'fact_loan_daily_snapshot',
            'dim_customer',          # Parent tables last
            'dim_date'
        ]
        
        for table in tables_to_clear:
            try:
                self.db.execute_query(f"DELETE FROM {table}")
                logger.info(f"   ✅ Cleared {table}")
            except Exception as e:
                logger.warning(f"   ⚠️  Could not clear {table}: {e}")
        
        self.db.execute_query("SET FOREIGN_KEY_CHECKS = 1")
        logger.info("🔒 Foreign key checks re-enabled")
        
        self.pipeline_results['date_dimension'] = self.run_date_dimension_etl()
        
        self.pipeline_results['customer_dimension'] = self.run_customer_dimension_etl()
        
        self.pipeline_results['loan_fact'] = self.run_loan_fact_etl()
        
        self.pipeline_results['transaction_fact'] = self.run_transaction_fact_etl()
        
        self.pipeline_results['fraud_alert_fact'] = self.run_fraud_alert_fact_etl()
        
        self.pipeline_results['verification'] = self.verify_load()
        
        self.end_time = datetime.now()
        duration = (self.end_time - self.start_time).total_seconds()
        
        logger.info("\n" + "=" * 60)
        logger.info("✅ ETL PIPELINE COMPLETED")
        logger.info("=" * 60)
        logger.info(f"End Time: {self.end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Total Duration: {duration:.2f} seconds ({duration/60:.2f} minutes)")
        logger.info("=" * 60)
        
        logger.info("\n📊 ETL SUMMARY:")
        for step, result in self.pipeline_results.items():
            if step != 'verification' and isinstance(result, dict):
                status_icon = "✅" if result.get('status') == 'SUCCESS' else "❌" if result.get('status') == 'FAILED' else "⚠️"
                logger.info(f"   {status_icon} {step}: {result.get('status', 'UNKNOWN')} - {result.get('records_loaded', 0):,} records")
        
        report = self.generate_etl_report()
        
        return report
    


if __name__ == "__main__":
    etl = CreditFlowETL()
    report = etl.run_all()
    
    print("\n" + "=" * 60)
    print(f"🏁 ETL Pipeline Status: {report['overall_status']}")
    print(f"⏱️  Duration: {report['duration_seconds']:.2f} seconds")
    print("=" * 60)