
echo "========================================="
echo "CreditFlow360 - ETL Pipeline"
echo "========================================="

source venv/bin/activate

export PYTHONPATH=$PYTHONPATH:$(pwd)

echo "Step 1: Generating synthetic data..."
python -c "
from src.data_generation.synthetic_data_generator import NBFCDataGenerator
generator = NBFCDataGenerator()
data = generator.generate_all()
print('✓ Data generation completed')
"

echo "Step 2: Cleaning and transforming data..."
python -c "
import pandas as pd
from src.etl_python.data_cleaner import DataCleaner, DataValidator

# Clean customers
df = pd.read_csv('data/raw_csv/customers.csv')
df_clean = DataCleaner.clean_customers(df)
df_clean.to_csv('data/processed/customers_clean.csv', index=False)

# Clean loans
df = pd.read_csv('data/raw_csv/loans.csv')
df_clean = DataCleaner.clean_loans(df)
df_clean.to_csv('data/processed/loans_clean.csv', index=False)

# Validate
validation = DataValidator.validate_customers(df_clean)
print('✓ Data cleaning completed')
"

echo "Step 3: Loading to database..."
python scripts/load_to_database.py

echo "Step 4: Calculating risk metrics..."
python -c "
from src.database.db_connection import DatabaseConnection
from src.risk_analytics.credit_scoring import CreditRiskAnalytics

db = DatabaseConnection(db_type='mysql')
risk = CreditRiskAnalytics(db)
metrics = risk.calculate_portfolio_risk_metrics()
print(f'✓ Risk metrics calculated - GNPA: {metrics[\"gnpa_ratio\"]:.2%}')
"

echo "Step 5: Running fraud detection..."
python -c "
from src.database.db_connection import DatabaseConnection
from src.risk_analytics.credit_scoring import FraudRiskEngine

db = DatabaseConnection(db_type='mysql')
fraud = FraudRiskEngine(db)
alerts = fraud.detect_fraud_patterns()
print(f'✓ Fraud detection completed - {len(alerts)} alerts')
"

echo "Step 6: Exporting for Tableau..."
python -c "
from src.dashboard.csv_exporter import TableauExporter

exporter = TableauExporter()
exporter.export_all()
print('✓ Tableau exports completed')
"

echo "========================================="
echo "ETL Pipeline Completed Successfully!"
echo "========================================="