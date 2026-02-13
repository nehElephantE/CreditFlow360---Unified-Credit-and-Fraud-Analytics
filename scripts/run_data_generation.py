import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.data_generation.synthetic_data_generator import NBFCDataGenerator
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    print("\n" + "="*60)
    print("🏦 CREDITFLOW360 - PRODUCTION DATA GENERATION")
    print("="*60)
    
    generator = NBFCDataGenerator(seed=42)
    
    generator.configure(
        num_customers=25000,      # 25K customers - enough for segmentation
        num_loans=75000,          # 75K loans - ~3 per customer
        num_transactions=300000,  # 300K transactions - realistic repayment history
        target_fraud_rate=0.03    # 3% fraud rate - industry standard
    )
    
    print(f"\n📊 Configuration:")
    print(f"   • Customers: {generator.config['num_customers']:,}")
    print(f"   • Loans: {generator.config['num_loans']:,}")
    print(f"   • Transactions: {generator.config['num_transactions']:,}")
    print(f"   • Target Fraud Rate: {generator.config['target_fraud_rate']*100}%")
    
    data = generator.generate_all(validate=True)
    
    print("\n" + "="*60)
    print("✅ DATA GENERATION COMPLETE!")
    print("="*60)
    print("\n📁 Files created:")
    print("   • data/raw_csv/customers.csv")
    print("   • data/raw_csv/loans.csv")  
    print("   • data/raw_csv/transactions.csv")
    print("   • data/raw_csv/fraud_alerts.csv")
    print("\n🎯 Next Step: Run Step 3 - ETL Pipeline")
    print("="*60)
    
    return data

if __name__ == "__main__":
    main()