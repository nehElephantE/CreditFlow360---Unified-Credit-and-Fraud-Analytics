import os
from pathlib import Path

def init_directories():
    dirs = [
        'data/raw_csv',
        'data/processed',
        'data/exports',
        'reports/executive',
        'reports/credit_risk',
        'reports/fraud',
        'reports/regulatory',
        'logs',
        'config',
    ]
    
    for dir_path in dirs:
        Path(dir_path).mkdir(parents=True, exist_ok=True)
        print(f"✅ Created: {dir_path}")

if __name__ == "__main__":
    init_directories()