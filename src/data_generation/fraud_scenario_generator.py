import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import uuid
from tqdm import tqdm
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FraudGenerator:
    
    def __init__(self, seed=42):
        random.seed(seed)
        np.random.seed(seed)
        
        self.fraud_types = {
            'Income Mismatch': 0.25,
            'Multiple Applications Same Collateral': 0.15,
            'Synthetic Identity': 0.12,
            'Shell Company': 0.08,
            'Early Payment Default': 0.20,
            'Identity Theft': 0.10,
            'Stolen Documents': 0.05,
            'Fake Employment': 0.05
        }
        
        self.alert_categories = {
            'Income Mismatch': 'Application Fraud',
            'Multiple Applications Same Collateral': 'Application Fraud',
            'Synthetic Identity': 'Identity Fraud',
            'Shell Company': 'Business Fraud',
            'Early Payment Default': 'Transaction Fraud',
            'Identity Theft': 'Identity Fraud',
            'Stolen Documents': 'Identity Fraud',
            'Fake Employment': 'Application Fraud'
        }
        
        self.detection_methods = {
            'Income Mismatch': 'Rule-based',
            'Multiple Applications Same Collateral': 'Network Analysis',
            'Synthetic Identity': 'Anomaly Detection',
            'Shell Company': 'External Data Verification',
            'Early Payment Default': 'Behavioral Analysis',
            'Identity Theft': 'Document Verification',
            'Stolen Documents': 'Document Verification',
            'Fake Employment': 'Employment Verification'
        }
        
        self.risk_levels = {
            'Critical': 0.15,
            'High': 0.25,
            'Medium': 0.40,
            'Low': 0.20
        }
        
        self.investigation_status = {
            'New': 0.30,
            'In Progress': 0.25,
            'Confirmed': 0.30,
            'False Positive': 0.15
        }
        
        self.rule_triggers = {
            'Income Mismatch': [
                'Annual income exceeds declared by >50%',
                'Credit score vs income mismatch',
                'Employment type incompatible with income level'
            ],
            'Multiple Applications Same Collateral': [
                'Same collateral used for multiple loans',
                'Collateral value inflated across applications',
                'Same property multiple valuations'
            ],
            'Synthetic Identity': [
                'No credit history for age>25',
                'Inconsistent address history',
                'Phone number associated with multiple identities'
            ],
            'Shell Company': [
                'Company registered recently',
                'No physical address verification',
                'GST verification failed'
            ],
            'Early Payment Default': [
                'Default within first 3 months',
                'No payments made since disbursement',
                'Immediate delinquency pattern'
            ],
            'Identity Theft': [
                'Multiple applications with same PAN',
                'Address mismatch across applications',
                'Unusual application timing'
            ]
        }
    
    def generate_fraud_alert_id(self) -> str:
        return f'FRD{uuid.uuid4().hex[:10].upper()}'
    
    def calculate_risk_score(self, fraud_type: str, loan_amount: float, 
                           credit_score: int, days_past_due: int = 0) -> tuple:
        
        base_score = 0
        
        # Base score by fraud type
        fraud_type_weights = {
            'Income Mismatch': 60,
            'Multiple Applications Same Collateral': 75,
            'Synthetic Identity': 85,
            'Shell Company': 80,
            'Early Payment Default': 55,
            'Identity Theft': 90,
            'Stolen Documents': 85,
            'Fake Employment': 70
        }
        
        base_score = fraud_type_weights.get(fraud_type, 50)
        
        # Adjust for loan amount
        if loan_amount > 10000000:
            base_score += 15
        elif loan_amount > 5000000:
            base_score += 10
        elif loan_amount > 1000000:
            base_score += 5
        
        # Adjust for credit score
        if credit_score < 550:
            base_score += 15
        elif credit_score < 650:
            base_score += 10
        elif credit_score < 750:
            base_score += 5
        
        # Adjust for DPD
        if days_past_due > 90:
            base_score += 20
        elif days_past_due > 60:
            base_score += 15
        elif days_past_due > 30:
            base_score += 10
        
        # Ensure within range
        risk_score = min(100, max(1, base_score + random.randint(-10, 10)))
        
        # Determine risk level
        if risk_score >= 80:
            risk_level = 'Critical'
        elif risk_score >= 60:
            risk_level = 'High'
        elif risk_score >= 40:
            risk_level = 'Medium'
        else:
            risk_level = 'Low'
        
        return risk_score, risk_level
    
    def generate_financial_impact(self, fraud_type: str, loan_amount: float) -> float:
        
        impact_percentages = {
            'Income Mismatch': 0.30,
            'Multiple Applications Same Collateral': 0.60,
            'Synthetic Identity': 0.80,
            'Shell Company': 0.70,
            'Early Payment Default': 0.40,
            'Identity Theft': 0.50,
            'Stolen Documents': 0.45,
            'Fake Employment': 0.35
        }
        
        impact_pct = impact_percentages.get(fraud_type, 0.50)
        impact = loan_amount * impact_pct * random.uniform(0.8, 1.2)
        
        return round(impact, 2)
    

    def generate_fraud_scenario_income_mismatch(self, loans_df: pd.DataFrame, 
                                               customers_df: pd.DataFrame) -> list:
        fraud_cases = []
        
        merged_df = loans_df.merge(
            customers_df[['customer_id', 'annual_income', 'credit_score']], 
            on='customer_id', 
            how='left'
        )
        
        high_risk_customers = customers_df[
            (customers_df['credit_score'] < 600) & 
            (customers_df['annual_income'] < 800000)
        ]['customer_id'].tolist()
        
        suspicious_loans = merged_df[
            merged_df['customer_id'].isin(high_risk_customers) &
            (merged_df['loan_amount'] > merged_df['annual_income'] * 3) &
            (merged_df['loan_status'].isin(['Active', 'Overdue']))
        ]
        
        for _, loan in suspicious_loans.iterrows():
            if random.random() < 0.15:  # 15% conversion rate
                customer = customers_df[customers_df['customer_id'] == loan['customer_id']].iloc[0]
                
                risk_score, risk_level = self.calculate_risk_score(
                    'Income Mismatch',
                    loan['loan_amount'],
                    customer['credit_score'],
                    loan['days_past_due']
                )
                
                alert = {
                    'alert_id': self.generate_fraud_alert_id(),
                    'loan_id': loan['loan_id'],
                    'customer_id': loan['customer_id'],
                    'transaction_id': None,
                    'detection_date': datetime.now() - timedelta(days=random.randint(1, 30)),
                    'alert_type': 'Income Mismatch',
                    'alert_category': self.alert_categories['Income Mismatch'],
                    'risk_score': risk_score,
                    'risk_level': risk_level,
                    'detection_method': self.detection_methods['Income Mismatch'],
                    'rule_triggered': random.choice(self.rule_triggers['Income Mismatch']),
                    'alert_description': f"Income of ₹{customer['annual_income']:,.0f} incompatible with loan amount of ₹{loan['loan_amount']:,.0f} and credit score of {customer['credit_score']}",
                    'assigned_to': f'ANALYST{random.randint(1, 20):02d}',
                    'investigation_status': random.choices(
                        list(self.investigation_status.keys()),
                        weights=list(self.investigation_status.values())
                    )[0],
                    'investigation_notes': None,
                    'resolution_date': None,
                    'financial_impact': self.generate_financial_impact('Income Mismatch', loan['loan_amount'])
                }
                fraud_cases.append(alert)
        
        return fraud_cases
    

    def generate_fraud_scenario_collateral_fraud(self, loans_df: pd.DataFrame) -> list:
        fraud_cases = []
        
        collateral_loans = loans_df[loans_df['collateral_id'].notna()].copy()
        
        if len(collateral_loans) == 0:
            return fraud_cases
        
        collateral_groups = collateral_loans.groupby('collateral_id')
        
        for collateral_id, group in collateral_groups:
            if len(group) >= 2 and random.random() < 0.3:
                unique_customers = group['customer_id'].nunique()
                
                if unique_customers >= 2:
                    total_amount = group['loan_amount'].sum()
                    
                    for _, loan in group.iterrows():
                        risk_score, risk_level = self.calculate_risk_score(
                            'Multiple Applications Same Collateral',
                            loan['loan_amount'],
                            650,  # Default credit score
                            loan['days_past_due']
                        )
                        
                        alert = {
                            'alert_id': self.generate_fraud_alert_id(),
                            'loan_id': loan['loan_id'],
                            'customer_id': loan['customer_id'],
                            'transaction_id': None,
                            'detection_date': datetime.now() - timedelta(days=random.randint(1, 15)),
                            'alert_type': 'Multiple Applications Same Collateral',
                            'alert_category': self.alert_categories['Multiple Applications Same Collateral'],
                            'risk_score': risk_score + 10,  # Higher risk for collateral fraud
                            'risk_level': 'High' if risk_score > 70 else risk_level,
                            'detection_method': self.detection_methods['Multiple Applications Same Collateral'],
                            'rule_triggered': random.choice(self.rule_triggers['Multiple Applications Same Collateral']),
                            'alert_description': f"Collateral {collateral_id} used for {len(group)} applications by {unique_customers} different customers",
                            'assigned_to': f'ANALYST{random.randint(1, 20):02d}',
                            'investigation_status': random.choices(
                                ['In Progress', 'Confirmed', 'False Positive'],
                                weights=[0.4, 0.4, 0.2]
                            )[0],
                            'investigation_notes': None,
                            'resolution_date': None,
                            'financial_impact': self.generate_financial_impact('Multiple Applications Same Collateral', total_amount)
                        }
                        fraud_cases.append(alert)
        
        return fraud_cases
    
    def generate_fraud_scenario_early_default(self, loans_df: pd.DataFrame) -> list:
        fraud_cases = []
        
        three_months_ago = (datetime.now() - timedelta(days=90)).date()
        
        early_default_loans = loans_df[
            (loans_df['disbursement_date'].notna()) &
            (loans_df['disbursement_date'] >= three_months_ago) &
            (loans_df['days_past_due'] > 30) &
            (loans_df['loan_status'].isin(['Overdue', 'NPA']))
        ]
        
        for _, loan in early_default_loans.iterrows():
            if random.random() < 0.25:  # 25% detection rate
                risk_score, risk_level = self.calculate_risk_score(
                    'Early Payment Default',
                    loan['loan_amount'],
                    loan['bureau_score_at_origination'] or 650,
                    loan['days_past_due']
                )
                
                alert = {
                    'alert_id': self.generate_fraud_alert_id(),
                    'loan_id': loan['loan_id'],
                    'customer_id': loan['customer_id'],
                    'transaction_id': None,
                    'detection_date': datetime.now().date() - timedelta(days=random.randint(1, 7)),
                    'alert_type': 'Early Payment Default',
                    'alert_category': self.alert_categories['Early Payment Default'],
                    'risk_score': risk_score,
                    'risk_level': risk_level,
                    'detection_method': self.detection_methods['Early Payment Default'],
                    'rule_triggered': random.choice(self.rule_triggers['Early Payment Default']),
                    'alert_description': f"Loan defaulted within first 3 months. DPD: {loan['days_past_due']} days",
                    'assigned_to': f'COLLECT{random.randint(1, 30):02d}',
                    'investigation_status': random.choices(
                        ['New', 'In Progress', 'Confirmed'],
                        weights=[0.5, 0.3, 0.2]
                    )[0],
                    'investigation_notes': None,
                    'resolution_date': None,
                    'financial_impact': self.generate_financial_impact('Early Payment Default', loan['loan_amount'])
                }
                fraud_cases.append(alert)
        
        return fraud_cases
    
    
    def generate_fraud_scenario_synthetic_id(self, customers_df: pd.DataFrame) -> list:
        fraud_cases = []
        
        cutoff_date = (datetime.now() - timedelta(days=180)).date()
        
        synthetic_candidates = customers_df[
            (customers_df['credit_score'] < 550) &
            (customers_df['age'] > 25) &
            (customers_df['acquisition_date'] > cutoff_date)
        ]
        
        for _, customer in synthetic_candidates.iterrows():
            if random.random() < 0.10:  # 10% detection rate
                alert = {
                    'alert_id': self.generate_fraud_alert_id(),
                    'loan_id': None,
                    'customer_id': customer['customer_id'],
                    'transaction_id': None,
                    'detection_date': datetime.now().date() - timedelta(days=random.randint(1, 20)),
                    'alert_type': 'Synthetic Identity',
                    'alert_category': self.alert_categories['Synthetic Identity'],
                    'risk_score': random.randint(75, 95),
                    'risk_level': 'High',
                    'detection_method': self.detection_methods['Synthetic Identity'],
                    'rule_triggered': random.choice(self.rule_triggers['Synthetic Identity']),
                    'alert_description': f"Customer age {customer['age']} with credit score {customer['credit_score']} and no credit history",
                    'assigned_to': f'ANALYST{random.randint(1, 20):02d}',
                    'investigation_status': random.choices(
                        ['New', 'In Progress'],
                        weights=[0.6, 0.4]
                    )[0],
                    'investigation_notes': None,
                    'resolution_date': None,
                    'financial_impact': 0  # Prevented fraud
                }
                fraud_cases.append(alert)
        
        return fraud_cases
    

    def generate_all_fraud_scenarios(self, loans_df: pd.DataFrame, 
                                    customers_df: pd.DataFrame) -> pd.DataFrame:
        logger.info("🚀 Generating fraud scenarios...")
        
        all_fraud_cases = []
        
        logger.info("📊 Generating income mismatch fraud...")
        all_fraud_cases.extend(self.generate_fraud_scenario_income_mismatch(loans_df, customers_df))
        
        logger.info("📊 Generating collateral fraud...")
        all_fraud_cases.extend(self.generate_fraud_scenario_collateral_fraud(loans_df))
        
        logger.info("📊 Generating early default fraud...")
        all_fraud_cases.extend(self.generate_fraud_scenario_early_default(loans_df))
        
        logger.info("📊 Generating synthetic identity fraud...")
        all_fraud_cases.extend(self.generate_fraud_scenario_synthetic_id(customers_df))
        
        df = pd.DataFrame(all_fraud_cases)
        
        if len(df) > 0:
            confirmed_mask = df['investigation_status'] == 'Confirmed'
            df.loc[confirmed_mask, 'investigation_notes'] = [
                f"Fraud confirmed after investigation. {random.choice(['Legal action initiated', 'Loan written off', 'Recovery in progress'])}"
                for _ in range(confirmed_mask.sum())
            ]
            
            df.loc[confirmed_mask, 'resolution_date'] = [
                datetime.now() - timedelta(days=random.randint(1, 30))
                for _ in range(confirmed_mask.sum())
            ]
        
        output_path = 'data/raw_csv/fraud_alerts.csv'
        df.to_csv(output_path, index=False)
        logger.info(f"💾 Saved {len(df)} fraud alerts to {output_path}")

        if len(df) > 0:
            fraud_loan_ids = df[df['loan_id'].notna()]['loan_id'].tolist()
            loans_df.loc[loans_df['loan_id'].isin(fraud_loan_ids), 'fraud_flag'] = True

            for _, alert in df[df['loan_id'].notna()].iterrows():
                mask = loans_df['loan_id'] == alert['loan_id']
                loans_df.loc[mask, 'fraud_type'] = alert['alert_type']
                loans_df.loc[mask, 'fraud_detection_date'] = alert['detection_date']

            loans_df.to_csv('data/raw_csv/loans.csv', index=False)
            logger.info(f"✅ Updated fraud flags for {len(fraud_loan_ids)} loans")
        
        return df


if __name__ == "__main__":
    from customer_generator import CustomerGenerator
    from loan_generator import LoanGenerator
    
    cust_gen = CustomerGenerator()
    customers = cust_gen.generate_customers(n=1000)
    
    loan_gen = LoanGenerator()
    loans = loan_gen.generate_loans(customers, n=200)
    
    fraud_gen = FraudGenerator()
    fraud_alerts = fraud_gen.generate_all_fraud_scenarios(loans, customers)
    
    print(f"\n✅ Generated {len(fraud_alerts)} fraud alerts")
    if len(fraud_alerts) > 0:
        print(f"\n📊 Sample data:")
        print(fraud_alerts[['alert_id', 'alert_type', 'risk_level', 
                           'investigation_status']].head())