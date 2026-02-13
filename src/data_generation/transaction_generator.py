import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import uuid
from tqdm import tqdm
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TransactionGenerator:
    
    def __init__(self, seed=42):
        random.seed(seed)
        np.random.seed(seed)
        
        self.transaction_modes = {
            'NEFT': 0.35,
            'UPI': 0.30,
            'RTGS': 0.10,
            'Cheque': 0.15,
            'Cash': 0.08,
            'DD': 0.02
        }
        
        self.transaction_types = {
            'EMI': 0.75,
            'Prepayment': 0.10,
            'Foreclosure': 0.02,
            'Penalty': 0.05,
            'Processing Fee': 0.05,
            'Disbursement': 0.03
        }
        
        self.banks = [
            'State Bank of India', 'HDFC Bank', 'ICICI Bank', 'Axis Bank',
            'Kotak Mahindra Bank', 'Yes Bank', 'IDFC First Bank', 'IndusInd Bank',
            'Bank of Baroda', 'Punjab National Bank', 'Canara Bank', 'Union Bank'
        ]
        
        self.transaction_status = {
            'Success': 0.98,
            'Failed': 0.01,
            'Pending': 0.01
        }
        
        self.failure_reasons = [
            'Insufficient funds', 'Invalid account number', 'Technical error',
            'Bank server timeout', 'Transaction limit exceeded',
            'Account blocked', 'Invalid IFSC code', 'Duplicate transaction'
        ]
    
    def generate_transaction_id(self) -> str:
        return f'TXN{datetime.now().strftime("%y%m%d")}{uuid.uuid4().hex[:8].upper()}'
    
    def calculate_emi_components(self, amount: float, interest_rate: float, 
                                outstanding: float, days_past_due: int = 0) -> dict:
        
        monthly_rate = interest_rate / 12 / 100
        
        if monthly_rate > 0:
            interest_component = outstanding * monthly_rate
            principal_component = amount - interest_component
        else:
            interest_component = 0
            principal_component = amount
        
        penalty_component = 0
        if days_past_due > 0:
            penalty_rate = 0.02  # 2% penalty
            penalty_component = amount * penalty_rate * (days_past_due / 30)
        
        gst_component = penalty_component * 0.18
        
        return {
            'principal': round(principal_component, 2),
            'interest': round(interest_component, 2),
            'penalty': round(penalty_component, 2),
            'gst': round(gst_component, 2)
        }
    
    def generate_transactions_for_loan(self, loan: dict, customer: dict) -> list:
        transactions = []
        
        if loan['disbursement_date'] is None or loan['loan_status'] == 'Rejected':
            return transactions
        
        disbursement_date = pd.to_datetime(loan['disbursement_date'])
        first_emi_date = pd.to_datetime(loan['first_emi_date'])
        end_date = datetime.now()
        
        disbursement_txn = {
            'transaction_id': self.generate_transaction_id(),
            'loan_id': loan['loan_id'],
            'customer_id': loan['customer_id'],
            'transaction_date': disbursement_date,
            'transaction_type': 'Disbursement',
            'transaction_mode': random.choices(
                list(self.transaction_modes.keys()),
                weights=list(self.transaction_modes.values())
            )[0],
            'amount': loan['net_disbursed_amount'] or loan['loan_amount'],
            'principal_component': None,
            'interest_component': None,
            'penalty_component': None,
            'gst_component': None,
            'payment_reference': f'REF{disbursement_date.strftime("%y%m%d")}{random.randint(1000, 9999)}',
            'bank_name': random.choice(self.banks),
            'bank_account_last4': str(random.randint(1000, 9999)),
            'transaction_status': 'Success',
            'failure_reason': None,
            'reconciliation_status': 'Matched',
            'reconciled_date': disbursement_date + timedelta(days=1)
        }
        transactions.append(disbursement_txn)
        
        months_since_disbursement = (end_date - disbursement_date).days // 30
        total_emis = int(min(months_since_disbursement, loan['tenure_months'] or 0)) 
        
        for month in range(1, total_emis + 1):
            emi_date = first_emi_date + timedelta(days=30 * (month - 1))
            
            if emi_date > end_date:
                continue

            if loan['days_past_due'] > 0 and month > (total_emis - 2):
                success_prob = 0.7  # Lower for recent EMIs if loan is in default
            else:
                success_prob = 0.95
            
            is_success = random.random() < success_prob
            
            components = self.calculate_emi_components(
                loan['emi_amount'],
                loan['interest_rate'],
                loan['current_balance'] * (0.9 ** (month / total_emis)),  # Decreasing balance
                loan['days_past_due'] if month == total_emis else 0
            )
            
            emi_txn = {
                'transaction_id': self.generate_transaction_id(),
                'loan_id': loan['loan_id'],
                'customer_id': loan['customer_id'],
                'transaction_date': emi_date,
                'transaction_type': 'EMI',
                'transaction_mode': random.choices(
                    list(self.transaction_modes.keys()),
                    weights=list(self.transaction_modes.values())
                )[0],
                'amount': loan['emi_amount'],
                'principal_component': components['principal'],
                'interest_component': components['interest'],
                'penalty_component': components['penalty'] if loan['days_past_due'] > 0 else 0,
                'gst_component': components['gst'] if loan['days_past_due'] > 0 else 0,
                'payment_reference': f'EMI{emi_date.strftime("%y%m%d")}{random.randint(1000, 9999)}',
                'bank_name': random.choice(self.banks),
                'bank_account_last4': str(random.randint(1000, 9999)),
                'transaction_status': 'Success' if is_success else random.choice(['Failed', 'Pending']),
                'failure_reason': random.choice(self.failure_reasons) if not is_success else None,
                'reconciliation_status': 'Matched' if is_success else 'Unmatched',
                'reconciled_date': emi_date + timedelta(days=1) if is_success else None
            }
            transactions.append(emi_txn)
        
        if random.random() < 0.2 and loan['loan_status'] == 'Active':
            prepayment_amount = loan['current_balance'] * random.uniform(0.2, 0.5)
            prepayment_date = end_date - timedelta(days=random.randint(1, 30))
            
            prepayment_txn = {
                'transaction_id': self.generate_transaction_id(),
                'loan_id': loan['loan_id'],
                'customer_id': loan['customer_id'],
                'transaction_date': prepayment_date,
                'transaction_type': 'Prepayment',
                'transaction_mode': random.choices(['NEFT', 'RTGS', 'UPI'], weights=[0.5, 0.3, 0.2])[0],
                'amount': round(prepayment_amount, 2),
                'principal_component': round(prepayment_amount, 2),
                'interest_component': 0,
                'penalty_component': 0,
                'gst_component': 0,
                'payment_reference': f'PRE{prepayment_date.strftime("%y%m%d")}{random.randint(1000, 9999)}',
                'bank_name': random.choice(self.banks),
                'bank_account_last4': str(random.randint(1000, 9999)),
                'transaction_status': 'Success',
                'failure_reason': None,
                'reconciliation_status': 'Matched',
                'reconciled_date': prepayment_date + timedelta(days=1)
            }
            transactions.append(prepayment_txn)
        
        return transactions
    
    def generate_transactions(self, loans_df: pd.DataFrame, customers_df: pd.DataFrame, 
                            n: int = 1000000) -> pd.DataFrame:
        logger.info(f"🚀 Generating {n} transactions...")
        
        all_transactions = []
        active_loans = loans_df[
            (loans_df['loan_status'].isin(['Active', 'Overdue', 'NPA'])) & 
            (loans_df['disbursement_date'].notna())
        ].copy()
        
        customers_dict = customers_df.set_index('customer_id').to_dict('index')
        
        total_transactions = 0
        target_transactions = n
        
        for idx, loan in tqdm(active_loans.iterrows(), total=len(active_loans), desc="Processing loans"):
            customer = customers_dict.get(loan['customer_id'], {})
            
            if not customer:
                continue
            
            # Generate transactions for this loan
            loan_transactions = self.generate_transactions_for_loan(loan, customer)
            all_transactions.extend(loan_transactions)
            total_transactions += len(loan_transactions)
            
            # Stop if we've reached target
            if total_transactions >= target_transactions:
                break
        
        df = pd.DataFrame(all_transactions)
        
        # Save to CSV
        output_path = 'data/raw_csv/transactions.csv'
        df.to_csv(output_path, index=False)
        logger.info(f"💾 Saved {len(df)} transactions to {output_path}")
        
        return df
    
    def generate_batch_transactions(self, loans_df: pd.DataFrame, customers_df: pd.DataFrame,
                                  total: int = 1000000) -> pd.DataFrame:
        return self.generate_transactions(loans_df, customers_df, n=total)


if __name__ == "__main__":
    from customer_generator import CustomerGenerator
    from loan_generator import LoanGenerator
    
    cust_gen = CustomerGenerator()
    customers = cust_gen.generate_customers(n=100)
    
    loan_gen = LoanGenerator()
    loans = loan_gen.generate_loans(customers, n=20)
    
    txn_gen = TransactionGenerator()
    transactions = txn_gen.generate_transactions(loans, customers, n=500)
    
    print(f"\n✅ Generated {len(transactions)} transactions")
    print(f"\n📊 Sample data:")
    print(transactions[['transaction_id', 'loan_id', 'transaction_date', 
                       'transaction_type', 'amount', 'transaction_status']].head())