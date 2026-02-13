import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import uuid
from tqdm import tqdm
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CollateralGenerator:
    
    def __init__(self, seed=42):
        random.seed(seed)
        np.random.seed(seed)
        
        self.collateral_types = {
            'Home Loan': [
                'Residential Apartment', 'Independent House', 'Villa', 
                'Commercial Shop', 'Office Space', 'Agricultural Land',
                'Residential Plot', 'Commercial Plot'
            ],
            'Auto Loan': [
                'New Car', 'Used Car', 'Commercial Vehicle', 
                'Two-wheeler', 'Construction Vehicle', 'Fleet Vehicle'
            ],
            'Business Loan': [
                'Factory Building', 'Warehouse', 'Machinery', 
                'Equipment', 'Inventory', 'Fixed Deposit',
                'Government Bonds', 'Mutual Funds', 'Shares'
            ],
            'Education Loan': [
                'Fixed Deposit', 'Residential Property', 'Land',
                'LIC Policy', 'Government Securities'
            ]
        }
        
        self.valuation_methods = {
            'Residential Apartment': 'Market Value - Registered Valuer',
            'Independent House': 'Market Value - Registered Valuer',
            'Commercial Shop': 'Income Method',
            'New Car': 'Invoice Value',
            'Used Car': 'Blue Book Value',
            'Machinery': 'Depreciated Cost',
            'Inventory': 'Cost or Market Value',
            'Fixed Deposit': 'Face Value',
            'Shares': 'Market Price'
        }
        
        self.conditions = ['Excellent', 'Good', 'Fair', 'Average']
        self.condition_weights = [0.3, 0.4, 0.2, 0.1]
        
        self.valuation_agencies = [
            'Knight Frank', 'CBRE', 'JLL', 'Cushman & Wakefield',
            'Colliers', 'Savills', 'ICRA Valuation', 'CARE Ratings'
        ]
        
    def generate_collateral_id(self) -> str:
        return f'COL{uuid.uuid4().hex[:10].upper()}'
    
    def calculate_collateral_value(self, loan_amount: float, product_type: str, 
                                 collateral_type: str) -> float:
    
        ltv_ratios = {
            'Home Loan': random.uniform(0.65, 0.80),
            'Auto Loan': random.uniform(0.75, 0.90),
            'Business Loan': random.uniform(0.60, 0.75),
            'Education Loan': random.uniform(0.85, 1.0)
        }
        
        base_ltv = ltv_ratios.get(product_type, 0.70)
        
        if 'Land' in collateral_type:
            ltv = base_ltv - 0.10  # Land has lower LTV
        elif 'Fixed Deposit' in collateral_type:
            ltv = base_ltv + 0.15  # FD has higher LTV
        elif 'Shares' in collateral_type:
            ltv = base_ltv - 0.15  # Shares have lower LTV due to volatility
        else:
            ltv = base_ltv
        
        # collateral value
        collateral_value = loan_amount / ltv
        
        collateral_value *= random.uniform(0.95, 1.05)
        
        return round(collateral_value, -3) 
    
    def generate_valuation_date(self, application_date: datetime) -> datetime:
        """Generate valuation date"""
        # Valuation typically done 1-2 weeks before application
        valuation_date = application_date - timedelta(days=random.randint(7, 15))
        return valuation_date
    
    def generate_insurance_details(self, collateral_type: str, 
                                 collateral_value: float) -> dict:
        
        # Only certain collateral types require insurance
        insurable_types = ['Residential Apartment', 'Independent House', 'Villa',
                          'New Car', 'Used Car', 'Commercial Vehicle',
                          'Factory Building', 'Warehouse', 'Machinery']
        
        if collateral_type not in insurable_types:
            return None
        
        # Insurance companies
        insurers = [
            'New India Assurance', 'United India Insurance', 'ICICI Lombard',
            'Bajaj Allianz', 'HDFC Ergo', 'Tata AIG', 'SBI General'
        ]
        
        # insurance premium (0.5% to 1.5% of collateral value)
        premium_rate = random.uniform(0.005, 0.015)
        annual_premium = collateral_value * premium_rate
        
        # Policy start and end dates
        start_date = datetime.now() - timedelta(days=random.randint(30, 90))
        end_date = start_date + timedelta(days=365)
        
        return {
            'insurance_company': random.choice(insurers),
            'policy_number': f'POL{uuid.uuid4().hex[:12].upper()}',
            'annual_premium': round(annual_premium, 2),
            'coverage_amount': collateral_value,
            'policy_start_date': start_date.date(),
            'policy_end_date': end_date.date(),
            'is_active': True
        }
    
    def generate_property_details(self, collateral_type: str, city: str) -> dict:
        
        if 'Residential' not in collateral_type and 'Commercial' not in collateral_type:
            return None
        
        # Property age distribution
        age_years = random.choices([0, 5, 10, 15, 20, 25], 
                                 weights=[0.2, 0.3, 0.25, 0.15, 0.07, 0.03])[0]
        
        # Construction type
        construction_types = ['RCC Framed', 'Load Bearing', 'Pre-fabricated']
        
        # Floor details
        total_floors = random.randint(1, 5)
        floor_no = random.randint(1, total_floors) if total_floors > 1 else 1
        
        # Furnishing status
        furnishing = random.choice(['Fully Furnished', 'Semi-Furnished', 'Unfurnished'])
        
        # Car parking
        parking = random.choice(['Covered', 'Open', 'None'], weights=[0.4, 0.4, 0.2])[0]
        
        return {
            'property_age_years': age_years,
            'construction_type': random.choice(construction_types),
            'total_floors': total_floors,
            'floor_no': floor_no,
            'furnishing_status': furnishing,
            'carpet_area_sqft': random.randint(500, 2500),
            'super_area_sqft': random.randint(600, 3000),
            'bedrooms': random.randint(1, 4),
            'bathrooms': random.randint(1, 4),
            'parking': parking,
            'ownership_type': random.choice(['Freehold', 'Leasehold']),
            'encumbrance': random.random() > 0.95  # 5% have encumbrance
        }
    
    def generate_vehicle_details(self, collateral_type: str) -> dict:
        
        if 'Car' not in collateral_type and 'Vehicle' not in collateral_type:
            return None
        
        car_brands = ['Maruti Suzuki', 'Hyundai', 'Tata', 'Mahindra', 'Honda',
                     'Toyota', 'Kia', 'MG', 'Skoda', 'Volkswagen']
        
        models = {
            'Maruti Suzuki': ['Swift', 'Baleno', 'Dzire', 'Vitara Brezza', 'Ertiga'],
            'Hyundai': ['i20', 'Creta', 'Verna', 'Venue', 'Grand i10'],
            'Tata': ['Nexon', 'Harrier', 'Tiago', 'Altroz', 'Safari'],
            'Mahindra': ['XUV500', 'Scorpio', 'Thar', 'Bolero', 'XUV300'],
            'Honda': ['City', 'Amaze', 'Civic', 'CR-V', 'WR-V'],
            'Toyota': ['Innova Crysta', 'Fortuner', 'Glanza', 'Camry'],
            'Kia': ['Seltos', 'Sonet', 'Carnival'],
            'MG': ['Hector', 'ZS EV', 'Gloster'],
            'Skoda': ['Octavia', 'Superb', 'Kushaq'],
            'Volkswagen': ['Polo', 'Vento', 'Taigun']
        }
        
        brand = random.choice(car_brands)
        model = random.choice(models[brand])
        
        current_year = datetime.now().year
        manufacture_year = random.randint(current_year - 5, current_year)
        
        registration_state = random.choice(['MH', 'KA', 'TN', 'DL', 'GJ', 'UP', 'WB'])
        registration_number = f"{registration_state}{random.randint(10,99)}{random.choice(['A','B','C','D','E'])}{random.randint(1000,9999)}"
        
        kms_driven = random.randint(5000, 80000) if manufacture_year < current_year else random.randint(100, 5000)
        
        return {
            'brand': brand,
            'model': model,
            'variant': random.choice(['Base', 'Mid', 'Top']),
            'fuel_type': random.choice(['Petrol', 'Diesel', 'CNG', 'Electric']),
            'transmission': random.choice(['Manual', 'Automatic']),
            'manufacture_year': manufacture_year,
            'registration_number': registration_number,
            'registration_state': registration_state,
            'kilometers_driven': kms_driven,
            'ownership': random.choice(['First', 'Second', 'Third']),
            'insurance_valid_till': (datetime.now() + timedelta(days=random.randint(30, 365))).date()
        }
    
    def generate_business_asset_details(self, collateral_type: str) -> dict:
        
        if 'Machinery' in collateral_type or 'Equipment' in collateral_type:
            asset_types = ['CNC Machine', 'Generator', 'Compressor', 'Packaging Machine',
                          'Textile Machinery', 'Printing Press', 'Medical Equipment']
        elif 'Warehouse' in collateral_type:
            asset_types = ['Godown', 'Cold Storage', 'Distribution Center']
        elif 'Inventory' in collateral_type:
            asset_types = ['Raw Material', 'Finished Goods', 'Spare Parts']
        else:
            return None
        
        return {
            'asset_type': random.choice(asset_types),
            'make': random.choice(['Indian', 'German', 'Japanese', 'Chinese', 'American']),
            'model_number': f'MOD{uuid.uuid4().hex[:6].upper()}',
            'serial_number': f'SER{uuid.uuid4().hex[:8].upper()}',
            'purchase_date': (datetime.now() - timedelta(days=random.randint(180, 1095))).date(),
            'purchase_cost': random.randint(100000, 5000000),
            'depreciation_rate': random.uniform(0.10, 0.25),
            'maintenance_status': random.choice(['Excellent', 'Good', 'Needs Service'])
        }
    
    def generate_collateral_package(self, loan_amount: float, product_type: str,
                                  application_date: datetime, city: str) -> dict:
        
        collateral_types = self.collateral_types.get(product_type, ['Fixed Deposit'])
        collateral_type = random.choice(collateral_types)
        
        collateral_id = self.generate_collateral_id()
        
        collateral_value = self.calculate_collateral_value(
            loan_amount, product_type, collateral_type
        )
        
        valuation_date = self.generate_valuation_date(application_date)
        valuation_agency = random.choice(self.valuation_agencies)
        valuer_name = f"VLR{random.randint(100, 999)}"
        
        # LTV ratio
        ltv_ratio = (loan_amount / collateral_value) * 100
        
        # Collateral condition
        condition = random.choices(self.conditions, weights=self.condition_weights)[0]
        
        insurance_details = self.generate_insurance_details(collateral_type, collateral_value)
        property_details = self.generate_property_details(collateral_type, city)
        vehicle_details = self.generate_vehicle_details(collateral_type)
        business_asset_details = self.generate_business_asset_details(collateral_type)
        
        # Ownership type
        ownership_types = ['Self-owned', 'Joint Ownership', 'Family-owned', 'Partnership']
        ownership = random.choice(ownership_types) if 'Property' in collateral_type else 'Self-owned'
        
        collateral = {
            'collateral_id': collateral_id,
            'collateral_type': collateral_type,
            'collateral_value': collateral_value,
            'valuation_date': valuation_date.date(),
            'valuation_agency': valuation_agency,
            'valuer_name': valuer_name,
            'valuation_report_number': f'VAL{uuid.uuid4().hex[:10].upper()}',
            'loan_to_value_ratio': round(ltv_ratio, 2),
            'condition': condition,
            'ownership_type': ownership,
            'ownership_verified': random.random() > 0.1,  # 90% verified
            'verification_date': (application_date - timedelta(days=random.randint(2, 5))).date(),
            'insurance_details': insurance_details,
            'property_details': property_details,
            'vehicle_details': vehicle_details,
            'business_asset_details': business_asset_details,
            'is_primary_collateral': True,
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        }
        
        return collateral
    
    def generate_multiple_collateral(self, loan_amount: float, product_type: str,
                                   application_date: datetime, city: str) -> list:

        
        collaterals = []
        
        if loan_amount > 5000000 and random.random() < 0.3:  # 30% of large loans
            # Primary collateral
            primary = self.generate_collateral_package(
                loan_amount * 0.7, product_type, application_date, city
            )
            primary['is_primary_collateral'] = True
            collaterals.append(primary)
            
            # Secondary collateral
            secondary = self.generate_collateral_package(
                loan_amount * 0.3, product_type, application_date, city
            )
            secondary['is_primary_collateral'] = False
            collaterals.append(secondary)
        else:
            # Single collateral
            collateral = self.generate_collateral_package(
                loan_amount, product_type, application_date, city
            )
            collaterals.append(collateral)
        
        return collaterals


if __name__ == "__main__":
    gen = CollateralGenerator()
    
    home_collateral = gen.generate_collateral_package(
        loan_amount=5000000,
        product_type='Home Loan',
        application_date=datetime.now(),
        city='Mumbai'
    )
    
    print("\n🏠 Home Loan Collateral:")
    print(f"Collateral ID: {home_collateral['collateral_id']}")
    print(f"Type: {home_collateral['collateral_type']}")
    print(f"Value: ₹{home_collateral['collateral_value']:,.0f}")
    print(f"LTV Ratio: {home_collateral['loan_to_value_ratio']:.1f}%")
    
    if home_collateral['property_details']:
        print(f"\n🏢 Property Details:")
        print(f"Area: {home_collateral['property_details']['carpet_area_sqft']} sq.ft")
        print(f"Bedrooms: {home_collateral['property_details']['bedrooms']}")
        print(f"Age: {home_collateral['property_details']['property_age_years']} years")
    
    auto_collateral = gen.generate_collateral_package(
        loan_amount=800000,
        product_type='Auto Loan',
        application_date=datetime.now(),
        city='Pune'
    )
    
    print("\n🚗 Auto Loan Collateral:")
    print(f"Collateral ID: {auto_collateral['collateral_id']}")
    print(f"Type: {auto_collateral['collateral_type']}")
    print(f"Value: ₹{auto_collateral['collateral_value']:,.0f}")
    
    if auto_collateral['vehicle_details']:
        print(f"\n🚘 Vehicle Details:")
        print(f"Brand: {auto_collateral['vehicle_details']['brand']}")
        print(f"Model: {auto_collateral['vehicle_details']['model']}")
        print(f"Year: {auto_collateral['vehicle_details']['manufacture_year']}")
        print(f"Reg No: {auto_collateral['vehicle_details']['registration_number']}")