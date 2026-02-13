import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
import uuid
from tqdm import tqdm
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CustomerGenerator:
    
    def __init__(self, seed=42):
        self.faker = Faker(['en_IN'])
        Faker.seed(seed)
        random.seed(seed)
        np.random.seed(seed)
        
        self.states = {
            'Maharashtra': ['Mumbai', 'Pune', 'Nagpur', 'Nashik', 'Aurangabad'],
            'Karnataka': ['Bangalore', 'Mysore', 'Hubli', 'Mangalore'],
            'Tamil Nadu': ['Chennai', 'Coimbatore', 'Madurai', 'Trichy'],
            'Delhi': ['New Delhi', 'Delhi'],
            'Gujarat': ['Ahmedabad', 'Surat', 'Vadodara', 'Rajkot'],
            'Telangana': ['Hyderabad', 'Warangal'],
            'West Bengal': ['Kolkata', 'Howrah', 'Durgapur'],
            'Uttar Pradesh': ['Lucknow', 'Kanpur', 'Noida', 'Agra', 'Varanasi'],
            'Rajasthan': ['Jaipur', 'Jodhpur', 'Udaipur'],
            'Madhya Pradesh': ['Bhopal', 'Indore', 'Gwalior'],
            'Haryana': ['Gurgaon', 'Faridabad', 'Panipat'],
            'Punjab': ['Chandigarh', 'Ludhiana', 'Amritsar'],
            'Kerala': ['Kochi', 'Thiruvananthapuram', 'Kozhikode'],
            'Bihar': ['Patna', 'Gaya'],
            'Odisha': ['Bhubaneswar', 'Cuttack']
        }
        
        self.employment_types = {
            'Salaried': 0.55,
            'Self-Employed Professional': 0.15,
            'Business Owner': 0.12,
            'Government Employee': 0.08,
            'Contractual': 0.05,
            'Retired': 0.03,
            'Homemaker': 0.02
        }
        
        self.education_levels = {
            "High School": 0.10,
            "Diploma": 0.15,
            "Bachelor's": 0.45,
            "Master's": 0.25,
            "PhD": 0.03,
            "Professional Certificate": 0.02
        }
        
        self.occupations = {
            'Salaried': [
                'IT Professional', 'Banking Professional', 'Teacher', 'Engineer',
                'Doctor', 'Lawyer', 'Accountant', 'Marketing Manager', 'HR Manager',
                'Operations Manager', 'Sales Executive', 'Administrative Staff'
            ],
            'Self-Employed Professional': [
                'Chartered Accountant', 'Architect', 'Consultant', 'Freelancer',
                'Interior Designer', 'Photographer', 'Event Planner'
            ],
            'Business Owner': [
                'Retail Shop Owner', 'Restaurant Owner', 'Manufacturer', 'Wholesaler',
                'Transport Contractor', 'Real Estate Developer', 'Clinic Owner'
            ],
            'Government Employee': [
                'Civil Servant', 'Teacher', 'Police Officer', 'Railway Employee',
                'Public Sector Officer', 'Defense Personnel'
            ]
        }
        
        self.acquisition_channels = [
            'Direct Visit', 'Online Application', 'Broker', 'Partner Referral',
            'Employee Referral', 'Customer Referral', 'Digital Marketing',
            'Branch Walk-in', 'Tele-calling', 'Email Campaign'
        ]
        
        self.customer_segments = [
            'Mass', 'Mass Affluent', 'Affluent', 'High Net Worth', 'Premium'
        ]
        
    def generate_income(self, age: int, employment_type: str) -> float:
        
        base_income = 0
        
        if employment_type == 'Salaried':
            if age < 25:
                base_income = random.uniform(250000, 400000)
            elif age < 35:
                base_income = random.uniform(400000, 800000)
            elif age < 50:
                base_income = random.uniform(800000, 1500000)
            else:
                base_income = random.uniform(600000, 1200000)
                
        elif employment_type == 'Self-Employed Professional':
            if age < 35:
                base_income = random.uniform(500000, 1000000)
            elif age < 50:
                base_income = random.uniform(1000000, 2500000)
            else:
                base_income = random.uniform(800000, 2000000)
                
        elif employment_type == 'Business Owner':
            if age < 35:
                base_income = random.uniform(600000, 1500000)
            elif age < 50:
                base_income = random.uniform(1500000, 5000000)
            else:
                base_income = random.uniform(1000000, 3000000)
                
        elif employment_type == 'Government Employee':
            base_income = random.uniform(400000, 1200000)
            
        elif employment_type == 'Retired':
            base_income = random.uniform(300000, 800000)
            
        else:
            base_income = random.uniform(200000, 500000)
        
        income = base_income * random.uniform(0.9, 1.1)
        
        return round(income / 1000) * 1000
    
    def generate_credit_score(self, age: int, income: float, employment_type: str) -> int:
        
        # Base score
        base_score = random.randint(600, 750)
        
        # Age factor
        if age > 35:
            base_score += 25
        if age > 50:
            base_score += 25
            
        # Income factor
        if income > 1000000:
            base_score += 30
        if income > 2500000:
            base_score += 30
        if income > 5000000:
            base_score += 40
            
        # Employment factor
        if employment_type in ['Government Employee', 'Salaried']:
            base_score += 30
        elif employment_type == 'Self-Employed Professional':
            base_score += 20
        elif employment_type == 'Business Owner':
            base_score += 10
            
        # Random variation
        score = base_score + random.randint(-50, 50)
        
        return max(300, min(900, score))
    
    def get_income_tier(self, income: float) -> str:
        if income < 300000:
            return 'Low'
        elif income < 600000:
            return 'Lower-Middle'
        elif income < 1200000:
            return 'Middle'
        elif income < 2400000:
            return 'Upper-Middle'
        elif income < 5000000:
            return 'High'
        else:
            return 'Affluent'
    
    def get_credit_tier(self, score: int) -> str:
        if score >= 750:
            return 'Prime'
        elif score >= 650:
            return 'Near-Prime'
        elif score >= 550:
            return 'Sub-Prime'
        else:
            return 'Deep-Subprime'
    
    def get_customer_value_tier(self, income: float, credit_score: int) -> str:
        value_score = (income / 100000) + (credit_score / 10)
        
        if value_score > 200:
            return 'Platinum'
        elif value_score > 150:
            return 'Gold'
        elif value_score > 100:
            return 'Silver'
        else:
            return 'Bronze'
    
    def generate_phone(self) -> str:
        return f"9{random.randint(0,9)}{random.randint(0,9)}{random.randint(0,9)}{random.randint(0,9)}{random.randint(0,9)}{random.randint(0,9)}{random.randint(0,9)}{random.randint(0,9)}{random.randint(0,9)}"
    
    def generate_pincode(self) -> str:
        return f"{random.randint(100000, 999999)}"
    
    def generate_customers(self, n: int = 50000) -> pd.DataFrame:
        logger.info(f"🚀 Generating {n} customer profiles...")
        
        customers = []
        
        for i in tqdm(range(n), desc="Creating customers"):
            age = random.randint(22, 70)
            gender = random.choices(['Male', 'Female', 'Other'], weights=[0.48, 0.48, 0.04])[0]
            marital_status = random.choices(
                ['Single', 'Married', 'Divorced', 'Widowed'],
                weights=[0.35, 0.55, 0.07, 0.03]
            )[0]
            
            state = random.choice(list(self.states.keys()))
            city = random.choice(self.states[state])
        
            employment_type = random.choices(
                list(self.employment_types.keys()),
                weights=list(self.employment_types.values())
            )[0]
            
            education = random.choices(
                list(self.education_levels.keys()),
                weights=list(self.education_levels.values())
            )[0]

            annual_income = self.generate_income(age, employment_type)
            credit_score = self.generate_credit_score(age, annual_income, employment_type)
            
            acquisition_date = self.faker.date_between(start_date='-3y', end_date='today')
            date_of_birth = datetime.now() - timedelta(days=age*365)
            
            if employment_type in ['Government Employee']:
                email_domain = 'gov.in'
            elif employment_type in ['Business Owner']:
                email_domain = random.choice(['business.com', 'enterprise.com', 'company.in'])
            else:
                email_domain = random.choice(['gmail.com', 'yahoo.com', 'outlook.com', 'hotmail.com'])
            
            customer = {
                'customer_id': f'CUST{str(i+1).zfill(8)}',
                'first_name': self.faker.first_name(),
                'last_name': self.faker.last_name(),
                'date_of_birth': date_of_birth.date(),
                'age': age,
                'gender': gender,
                'marital_status': marital_status,
                'education': education,
                'employment_type': employment_type,
                'annual_income': annual_income,
                'income_tier': self.get_income_tier(annual_income),
                'credit_score': credit_score,
                'credit_tier': self.get_credit_tier(credit_score),
                'city': city,
                'state': state,
                'pincode': self.generate_pincode(),
                'address_line1': self.faker.street_address(),
                'address_line2': f"{self.faker.building_number()}, {self.faker.street_name()}" if random.random() > 0.5 else None,
                'phone': self.generate_phone(),
                'email': self.faker.email(domain=email_domain),
                'customer_segment': random.choice(self.customer_segments),
                'customer_value_tier': self.get_customer_value_tier(annual_income, credit_score),
                'acquisition_date': acquisition_date,
                'acquisition_channel': random.choice(self.acquisition_channels),
                'is_active': random.random() > 0.05,  # 95% active
                'effective_start_date': acquisition_date,
                'effective_end_date': None,
                'is_current': True
            }
            
            customers.append(customer)
            
            if (i + 1) % 10000 == 0:
                logger.info(f"✅ Generated {i + 1} customers")
        
        df = pd.DataFrame(customers)
        
        output_path = 'data/raw_csv/customers.csv'
        df.to_csv(output_path, index=False)
        logger.info(f"💾 Saved {len(df)} customers to {output_path}")
        
        return df
    
    def generate_batch_customers(self, batch_size: int = 5000, total: int = 50000) -> list:
        dfs = []
        batches = total // batch_size + (1 if total % batch_size else 0)
        
        for i in range(batches):
            n = min(batch_size, total - i * batch_size)
            logger.info(f"📦 Generating batch {i+1}/{batches} ({n} customers)")
            df = self.generate_customers(n=n)
            dfs.append(df)
        
        return pd.concat(dfs, ignore_index=True)


if __name__ == "__main__":
    generator = CustomerGenerator()
    df = generator.generate_customers(n=100)  # Test with 100
    print(f"\n✅ Generated {len(df)} customers")
    print(f"\n📊 Sample data:")
    print(df[['customer_id', 'first_name', 'last_name', 'annual_income', 'credit_score']].head())