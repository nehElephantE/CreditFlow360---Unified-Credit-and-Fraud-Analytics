import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import uuid
from tqdm import tqdm
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LoanGenerator:
    
    def __init__(self, seed=42):
        random.seed(seed)
        np.random.seed(seed)
        
        self.products = [
            # Home Loans
            {'product_id': 'HL001', 'product_name': 'Home Loan Prime', 'type': 'Home Loan', 
             'min_rate': 8.5, 'max_rate': 10.5, 'min_amount': 1000000, 'max_amount': 10000000,
             'min_tenure': 60, 'max_tenure': 240, 'collateral': True},
            {'product_id': 'HL002', 'product_name': 'Home Loan Premium', 'type': 'Home Loan',
             'min_rate': 8.0, 'max_rate': 9.5, 'min_amount': 2000000, 'max_amount': 20000000,
             'min_tenure': 60, 'max_tenure': 300, 'collateral': True},
            {'product_id': 'HL003', 'product_name': 'Home Loan Plus', 'type': 'Home Loan',
             'min_rate': 8.75, 'max_rate': 11.0, 'min_amount': 500000, 'max_amount': 5000000,
             'min_tenure': 36, 'max_tenure': 180, 'collateral': True},
            
            # Auto Loans
            {'product_id': 'AL001', 'product_name': 'New Car Loan', 'type': 'Auto Loan',
             'min_rate': 9.5, 'max_rate': 12.0, 'min_amount': 200000, 'max_amount': 3000000,
             'min_tenure': 12, 'max_tenure': 84, 'collateral': True},
            {'product_id': 'AL002', 'product_name': 'Used Car Loan', 'type': 'Auto Loan',
             'min_rate': 11.0, 'max_rate': 14.0, 'min_amount': 100000, 'max_amount': 2000000,
             'min_tenure': 12, 'max_tenure': 60, 'collateral': True},
            
            # Personal Loans
            {'product_id': 'PL001', 'product_name': 'Personal Loan Standard', 'type': 'Personal Loan',
             'min_rate': 11.0, 'max_rate': 16.0, 'min_amount': 50000, 'max_amount': 1500000,
             'min_tenure': 6, 'max_tenure': 60, 'collateral': False},
            {'product_id': 'PL002', 'product_name': 'Personal Loan Premium', 'type': 'Personal Loan',
             'min_rate': 10.5, 'max_rate': 14.0, 'min_amount': 100000, 'max_amount': 2500000,
             'min_tenure': 12, 'max_tenure': 72, 'collateral': False},
            
            # Business Loans
            {'product_id': 'BL001', 'product_name': 'Business Term Loan', 'type': 'Business Loan',
             'min_rate': 10.0, 'max_rate': 14.0, 'min_amount': 500000, 'max_amount': 50000000,
             'min_tenure': 12, 'max_tenure': 120, 'collateral': True},
            {'product_id': 'BL002', 'product_name': 'Working Capital Loan', 'type': 'Business Loan',
             'min_rate': 11.0, 'max_rate': 15.0, 'min_amount': 100000, 'max_amount': 20000000,
             'min_tenure': 6, 'max_tenure': 60, 'collateral': True},
            
            # Education Loans
            {'product_id': 'EL001', 'product_name': 'Education Loan Domestic', 'type': 'Education Loan',
             'min_rate': 8.0, 'max_rate': 11.0, 'min_amount': 100000, 'max_amount': 5000000,
             'min_tenure': 12, 'max_tenure': 120, 'collateral': False},
            {'product_id': 'EL002', 'product_name': 'Education Loan International', 'type': 'Education Loan',
             'min_rate': 8.5, 'max_rate': 12.0, 'min_amount': 500000, 'max_amount': 20000000,
             'min_tenure': 12, 'max_tenure': 180, 'collateral': True}
        ]
        
        self.product_dict = {p['product_id']: p for p in self.products}
        
        # Loan purposes
        self.loan_purposes = {
            'Home Loan': [
                'Purchase of new house', 'Home construction', 'Home renovation',
                'Extension of existing house', 'Plot purchase with construction'
            ],
            'Auto Loan': [
                'Purchase of new car', 'Purchase of used car', 'Purchase of two-wheeler',
                'Commercial vehicle purchase', 'Refinance existing auto loan'
            ],
            'Personal Loan': [
                'Medical emergency', 'Wedding expenses', 'Travel expenses',
                'Debt consolidation', 'Home appliances purchase', 'Education expenses'
            ],
            'Business Loan': [
                'Working capital requirement', 'Business expansion', 'Equipment purchase',
                'Inventory funding', 'New branch setup', 'Technology upgrade'
            ],
            'Education Loan': [
                'Undergraduate studies', 'Postgraduate studies', 'Professional certification',
                'Study abroad program', 'Research fellowship'
            ]
        }
        
        # DPD buckets distribution
        self.dpd_distribution = {
            0: 0.70,      # 70% current
            1: 0.12,      # 12% 1-30 days
            2: 0.08,      # 8% 31-60 days
            3: 0.05,      # 5% 61-90 days
            4: 0.05       # 5% 90+ days
        }
        
        # Loan status based on DPD
        self.loan_status_mapping = {
            0: 'Active',
            1: 'Active',
            2: 'Overdue',
            3: 'Overdue',
            4: 'NPA'
        }
    
    def calculate_emi(self, amount: float, rate: float, tenure: int) -> float:
        monthly_rate = rate / 12 / 100
        if monthly_rate == 0:
            return amount / tenure
        emi = amount * monthly_rate * (1 + monthly_rate)**tenure / ((1 + monthly_rate)**tenure - 1)
        return round(emi, 2)
    
    def calculate_eligibility(self, customer: dict, product: dict) -> dict:
        
        annual_income = customer['annual_income']
        credit_score = customer['credit_score']
        
        max_income_multiple = 3.0
        if credit_score > 750:
            max_income_multiple = 5.0
        elif credit_score > 650:
            max_income_multiple = 4.0
        
        max_eligible_amount = annual_income * max_income_multiple
        
        max_amount = min(product['max_amount'], max_eligible_amount)
        min_amount = product['min_amount']

        base_rate = (product['min_rate'] + product['max_rate']) / 2
        if credit_score > 750:
            rate = base_rate - 0.5
        elif credit_score < 600:
            rate = base_rate + 1.0
        else:
            rate = base_rate
        
        rate = max(product['min_rate'], min(product['max_rate'], rate))

        if 'Home' in product['type']:
            tenure = min(product['max_tenure'], 240)
        elif 'Auto' in product['type']:
            tenure = min(product['max_tenure'], 60)
        else:
            tenure = min(product['max_tenure'], 48)
        
        return {
            'eligible': True,
            'max_amount': max_amount,
            'min_amount': min_amount,
            'suggested_rate': round(rate, 2),
            'suggested_tenure': tenure
        }
    
    def generate_dpd(self) -> tuple:
        bucket = random.choices(
            list(self.dpd_distribution.keys()),
            weights=list(self.dpd_distribution.values())
        )[0]
        
        if bucket == 0:
            days = 0
        elif bucket == 1:
            days = random.randint(1, 30)
        elif bucket == 2:
            days = random.randint(31, 60)
        elif bucket == 3:
            days = random.randint(61, 90)
        else:
            days = random.randint(91, 180)
        
        return days, bucket
    
    def get_dpd_bucket(self, days: int) -> str:
        if days == 0:
            return '0'
        elif days <= 30:
            return '1-30'
        elif days <= 60:
            return '31-60'
        elif days <= 90:
            return '61-90'
        else:
            return '90+'
    
    def calculate_pd(self, credit_score: int, dpd_days: int = 0) -> float:
        base_pd = 1 / (1 + np.exp((credit_score - 650) / 50))
        
        # Adjust for DPD
        if dpd_days > 0:
            dpd_factor = min(0.3, dpd_days * 0.002)
        else:
            dpd_factor = 0
        
        pd = min(0.99, base_pd + dpd_factor)
        return round(pd, 4)
    
    def calculate_lgd(self, product_type: str, collateral_value: float = None, loan_amount: float = None) -> float:
        if collateral_value and loan_amount:
            ltv = loan_amount / collateral_value
            if ltv < 0.5:
                lgd = random.uniform(0.15, 0.30)
            elif ltv < 0.7:
                lgd = random.uniform(0.30, 0.50)
            else:
                lgd = random.uniform(0.50, 0.70)
        elif 'Home' in product_type:
            lgd = random.uniform(0.20, 0.40)
        elif 'Auto' in product_type:
            lgd = random.uniform(0.30, 0.50)
        else:
            lgd = random.uniform(0.60, 0.80)
        
        return round(lgd, 4)
    
    def generate_collateral(self, loan_amount: float, product_type: str) -> dict:
        if 'Home' in product_type:
            collateral_types = ['Residential Property', 'Commercial Property', 'Land']
            collateral_multiplier = random.uniform(1.2, 1.5)
        elif 'Auto' in product_type:
            collateral_types = ['Car', 'Commercial Vehicle', 'Two-wheeler']
            collateral_multiplier = random.uniform(1.1, 1.3)
        elif 'Business' in product_type:
            collateral_types = ['Commercial Property', 'Equipment', 'Inventory', 'Fixed Deposit']
            collateral_multiplier = random.uniform(1.2, 1.6)
        else:
            return None
        
        collateral_value = loan_amount * collateral_multiplier
        ltv = (loan_amount / collateral_value) * 100
        
        return {
            'collateral_id': f'COL{uuid.uuid4().hex[:8].upper()}',
            'collateral_type': random.choice(collateral_types),
            'collateral_value': round(collateral_value, -3),
            'loan_to_value_ratio': round(ltv, 2)
        }
    
    def generate_loans(self, customers_df: pd.DataFrame, branches_df: pd.DataFrame = None, n: int = 200000) -> pd.DataFrame:
        logger.info(f"🚀 Generating {n} loan applications...")
        
        loans = []
        active_customers = customers_df[customers_df['is_active'] == True].copy()
        
        if branches_df is not None and not branches_df.empty:
            branch_ids = branches_df['branch_id'].tolist()
        else:
            branch_ids = [f'BR{str(i).zfill(3)}' for i in range(1, 26)]
        
        start_date = datetime(2022, 1, 1)
        end_date = datetime.now()
        date_range_days = (end_date - start_date).days
        
        for i in tqdm(range(n), desc="Creating loans"):
    
            customer = active_customers.sample(1).iloc[0]
        
            if customer['annual_income'] > 2000000:
                product = random.choice([p for p in self.products if p['min_amount'] > 1000000] or self.products)
            elif customer['employment_type'] in ['Business Owner', 'Self-Employed Professional']:
                product = random.choice([p for p in self.products if 'Business' in p['type']] or self.products)
            else:
                product = random.choice(self.products)
            
            eligibility = self.calculate_eligibility(customer, product)
            
            if not eligibility['eligible']:
                continue
            
            loan_amount = random.uniform(
                min(product['min_amount'], eligibility['max_amount']),
                min(product['max_amount'], eligibility['max_amount'])
            )
            loan_amount = round(loan_amount / 1000) * 1000
        
            application_date = start_date + timedelta(days=random.randint(0, date_range_days))
            
            approved = random.random() < 0.9
            
            if approved:
                disbursement_date = application_date + timedelta(days=random.randint(5, 15))
                first_emi_date = disbursement_date + timedelta(days=30)
                
                days_past_due, dpd_bucket = self.generate_dpd()
                loan_status = self.loan_status_mapping[dpd_bucket] if dpd_bucket in self.loan_status_mapping else 'Active'
                npa_flag = days_past_due > 90
                
                # Calculate EMI
                emi_amount = self.calculate_emi(
                    loan_amount, 
                    eligibility['suggested_rate'], 
                    eligibility['suggested_tenure']
                )
                
                # Current balance calculation
                months_passed = (end_date - disbursement_date).days / 30
                paid_emis = min(months_passed, eligibility['suggested_tenure'])
                current_balance = max(0, loan_amount - (emi_amount * paid_emis * 0.7))  # 70% principal, 30% interest
                
                # Overdue amount
                if days_past_due > 0:
                    overdue_emis = days_past_due // 30
                    overdue_amount = emi_amount * overdue_emis
                else:
                    overdue_amount = 0
                
                # Collateral for secured loans
                collateral = None
                if product['collateral'] and random.random() < 0.8:
                    collateral = self.generate_collateral(loan_amount, product['type'])
                
                # Risk metrics
                pd_score = self.calculate_pd(customer['credit_score'], days_past_due)
                lgd_score = self.calculate_lgd(
                    product['type'],
                    collateral['collateral_value'] if collateral else None,
                    loan_amount
                )
                
                # Collection tier
                if days_past_due > 90 or pd_score > 0.7:
                    collection_tier = 3
                elif days_past_due > 30 or pd_score > 0.4:
                    collection_tier = 2
                else:
                    collection_tier = 1
                
                # Co-applicant (20% of loans)
                co_applicant = random.random() < 0.2
                
                loan = {
                    'loan_id': f'LOAN{str(i+1).zfill(10)}',
                    'customer_id': customer['customer_id'],
                    'product_id': product['product_id'],
                    'branch_id': random.choice(branch_ids),
                    'application_date': application_date.date(),
                    'disbursement_date': disbursement_date.date(),
                    'first_emi_date': first_emi_date.date(),
                    'loan_amount': loan_amount,
                    'sanctioned_amount': loan_amount,
                    'interest_rate': eligibility['suggested_rate'],
                    'tenure_months': eligibility['suggested_tenure'],
                    'emi_amount': emi_amount,
                    'processing_fee': round(loan_amount * 0.01, 2),
                    'gst_on_fee': round(loan_amount * 0.01 * 0.18, 2),
                    'net_disbursed_amount': loan_amount - round(loan_amount * 0.01 * 1.18, 2),
                    'loan_purpose': random.choice(self.loan_purposes.get(product['type'], ['General purpose'])),
                    'collateral_id': collateral['collateral_id'] if collateral else None,
                    'collateral_value': collateral['collateral_value'] if collateral else None,
                    'loan_to_value_ratio': collateral['loan_to_value_ratio'] if collateral else None,
                    'co_applicant_present': co_applicant,
                    'co_applicant_income': random.randint(200000, 1000000) if co_applicant else None,
                    'bureau_score_at_origination': customer['credit_score'],
                    'internal_risk_rating': customer['credit_tier'],
                    'probability_of_default': pd_score,
                    'loss_given_default': lgd_score,
                    'exposure_at_default': loan_amount * 0.9,  # 90% of loan at default
                    'expected_loss': loan_amount * pd_score * lgd_score,
                    'current_balance': round(current_balance, 2),
                    'overdue_amount': round(overdue_amount, 2),
                    'days_past_due': days_past_due,
                    'dpd_bucket': self.get_dpd_bucket(days_past_due),
                    'npa_flag': npa_flag,
                    'npa_date': disbursement_date + timedelta(days=days_past_due) if npa_flag else None,
                    'restructuring_flag': random.random() < 0.01 if days_past_due > 60 else False,
                    'written_off_flag': random.random() < 0.005 if days_past_due > 180 else False,
                    'loan_status': loan_status,
                    'collection_tier': collection_tier,
                    'assigned_collection_agent': f'AGENT{str(random.randint(1, 50)).zfill(3)}' if days_past_due > 30 else None,
                    'fraud_flag': False,  # Will be set by fraud generator
                    'fraud_type': None,
                    'fraud_detection_date': None
                }
            else:
                # Rejected loan
                loan = {
                    'loan_id': f'LOAN{uuid.uuid4().hex[:10].upper()}',
                    'customer_id': customer['customer_id'],
                    'product_id': product['product_id'],
                    'branch_id': random.choice(branch_ids),
                    'application_date': application_date.date(),
                    'disbursement_date': None,
                    'first_emi_date': None,
                    'loan_amount': loan_amount,
                    'sanctioned_amount': None,
                    'interest_rate': None,
                    'tenure_months': None,
                    'emi_amount': None,
                    'processing_fee': None,
                    'gst_on_fee': None,
                    'net_disbursed_amount': None,
                    'loan_purpose': random.choice(self.loan_purposes.get(product['type'], ['General purpose'])),
                    'collateral_id': None,
                    'collateral_value': None,
                    'loan_to_value_ratio': None,
                    'co_applicant_present': False,
                    'co_applicant_income': None,
                    'bureau_score_at_origination': customer['credit_score'],
                    'internal_risk_rating': customer['credit_tier'],
                    'probability_of_default': None,
                    'loss_given_default': None,
                    'exposure_at_default': None,
                    'expected_loss': None,
                    'current_balance': 0,
                    'overdue_amount': 0,
                    'days_past_due': 0,
                    'dpd_bucket': '0',
                    'npa_flag': False,
                    'npa_date': None,
                    'restructuring_flag': False,
                    'written_off_flag': False,
                    'loan_status': 'Rejected',
                    'collection_tier': None,
                    'assigned_collection_agent': None,
                    'fraud_flag': False,
                    'fraud_type': None,
                    'fraud_detection_date': None
                }
            
            loans.append(loan)
            
            if (i + 1) % 50000 == 0:
                logger.info(f"✅ Generated {i + 1} loans")
        
        df = pd.DataFrame(loans)
        
        output_path = 'data/raw_csv/loans.csv'
        df.to_csv(output_path, index=False)
        logger.info(f"💾 Saved {len(df)} loans to {output_path}")
        
        return df
    
    def generate_batch_loans(self, customers_df: pd.DataFrame, branches_df: pd.DataFrame = None, 
                           batch_size: int = 10000, total: int = 200000) -> pd.DataFrame:
        dfs = []
        batches = total // batch_size + (1 if total % batch_size else 0)
        
        for i in range(batches):
            n = min(batch_size, total - i * batch_size)
            logger.info(f"📦 Generating batch {i+1}/{batches} ({n} loans)")
            df = self.generate_loans(customers_df, branches_df, n=n)
            dfs.append(df)
        
        return pd.concat(dfs, ignore_index=True)


if __name__ == "__main__":
    from customer_generator import CustomerGenerator
    
    cust_gen = CustomerGenerator()
    customers = cust_gen.generate_customers(n=1000)
    
    loan_gen = LoanGenerator()
    loans = loan_gen.generate_loans(customers, n=100)
    
    print(f"\n✅ Generated {len(loans)} loans")
    print(f"\n📊 Sample data:")
    print(loans[['loan_id', 'customer_id', 'loan_amount', 'interest_rate', 'loan_status']].head())