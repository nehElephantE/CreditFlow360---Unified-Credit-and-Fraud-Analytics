import pandas as pd
import numpy as np
from datetime import datetime
import logging
import time
import os

from src.data_generation.customer_generator import CustomerGenerator
from src.data_generation.loan_generator import LoanGenerator
from src.data_generation.transaction_generator import TransactionGenerator
from src.data_generation.fraud_scenario_generator import FraudGenerator
from src.data_generation.data_validator import DataValidator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class NBFCDataGenerator:
    
    def __init__(self, seed=42):
        self.seed = seed
        self.customer_gen = CustomerGenerator(seed=seed)
        self.loan_gen = LoanGenerator(seed=seed)
        self.transaction_gen = TransactionGenerator(seed=seed)
        self.fraud_gen = FraudGenerator(seed=seed)
        
        self.config = {
            'num_customers': 50000,
            'num_loans': 200000,
            'num_transactions': 1000000,
            'target_fraud_rate': 0.03  # 3% fraud rate
        }
    
    def configure(self, **kwargs):
        self.config.update(kwargs)
        logger.info(f"⚙️  Configuration updated: {self.config}")
    
    def create_directories(self):
        directories = [
            'data/raw_csv',
            'data/processed',
            'data/exports',
            'data/aggregated',
            'logs'
        ]
        
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
        
        logger.info("📁 Directories created/verified")
    
    def generate_all(self, validate: bool = True) -> dict:
        
        start_time = time.time()
        logger.info("="*60)
        logger.info("🚀 CREDITFLOW360 - SYNTHETIC DATA GENERATION")
        logger.info("="*60)
        
        self.create_directories()
        
        logger.info("\n📊 STEP 1: Generating Customers")
        logger.info("-" * 40)
        customers = self.customer_gen.generate_customers(
            n=self.config['num_customers']
        )
        
        logger.info("\n📊 STEP 2: Generating Loans")
        logger.info("-" * 40)
        loans = self.loan_gen.generate_batch_loans(
            customers_df=customers,
            total=self.config['num_loans']
        )
        
        logger.info("\n📊 STEP 3: Generating Transactions")
        logger.info("-" * 40)
        transactions = self.transaction_gen.generate_batch_transactions(
            loans_df=loans,
            customers_df=customers,
            total=self.config['num_transactions']
        )
        
        logger.info("\n📊 STEP 4: Generating Fraud Scenarios")
        logger.info("-" * 40)
        fraud_alerts = self.fraud_gen.generate_all_fraud_scenarios(
            loans_df=loans,
            customers_df=customers
        )
        
        logger.info("\n📊 STEP 5: Validating Data Quality")
        logger.info("-" * 40)
        
        quality_report = None
        if validate:
            quality_report = DataValidator.generate_quality_report(
                customers, loans, transactions, fraud_alerts
            )
        
        end_time = time.time()
        duration = end_time - start_time
        
        logger.info("\n" + "="*60)
        logger.info("✅ GENERATION COMPLETE!")
        logger.info("="*60)
        logger.info(f"\n📈 SUMMARY STATISTICS:")
        logger.info(f"   • Customers: {len(customers):,}")
        logger.info(f"   • Loans: {len(loans):,}")
        logger.info(f"   • Transactions: {len(transactions):,}")
        logger.info(f"   • Fraud Alerts: {len(fraud_alerts):,}")
        logger.info(f"\n   • Approved Loans: {loans[loans['disbursement_date'].notna()].shape[0]:,}")
        logger.info(f"   • Rejected Loans: {loans[loans['loan_status'] == 'Rejected'].shape[0]:,}")
        logger.info(f"   • Active Loans: {loans[loans['loan_status'].isin(['Active', 'Overdue'])].shape[0]:,}")
        logger.info(f"   • NPA Loans: {loans[loans['npa_flag'] == True].shape[0]:,}")
        logger.info(f"   • Fraud Loans: {loans[loans['fraud_flag'] == True].shape[0]:,}")
        
        if quality_report:
            logger.info(f"\n   • Data Quality Score: {quality_report['overall_quality_score']:.1%}")
        
        logger.info(f"\n⏱️  Generation Time: {duration:.2f} seconds ({duration/60:.2f} minutes)")
        logger.info("\n💾 Data saved to: data/raw_csv/")
        logger.info("="*60)
        
        return {
            'customers': customers,
            'loans': loans,
            'transactions': transactions,
            'fraud_alerts': fraud_alerts,
            'quality_report': quality_report,
            'generation_time': duration,
            'config': self.config
        }


if __name__ == "__main__":
    generator = NBFCDataGenerator(seed=42)
    
    generator.configure(
        num_customers=1000,
        num_loans=5000,
        num_transactions=25000
    )
    
    data = generator.generate_all(validate=True)