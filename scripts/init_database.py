import sys
import os
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.database.db_connection import DatabaseConnection, test_database_connection
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    print("\n" + "="*60)
    print("🚀 CREDITFLOW360 - DATABASE INITIALIZATION")
    print("="*60)
    
    print("\n📡 Step 1: Testing database connection...")
    if not test_database_connection():
        print("\n❌ Database connection failed!")
        print("\n💡 QUICK FIX:")
        print("   1. Open config/database.ini")
        print("   2. Set your MySQL password:")
        print("      password = YOUR_PASSWORD_HERE")
        print("   3. Make sure MySQL is running")
        print("   4. Run this script again")
        return False
    
    print("\n🔌 Step 2: Initializing database connection...")
    db = DatabaseConnection()
    
    print("\n🗄️  Step 3: Creating database if not exists...")
    if db.create_database():
        print(f"   ✅ Database '{db.config['database']}' is ready")
    else:
        print(f"   ❌ Failed to create database")
        return False
    
    print("\n" + "="*60)
    print("✅ DATABASE INITIALIZATION COMPLETE!")
    print("="*60)
    print("\n📊 Next Steps:")
    print("   1. Run: python scripts/create_schema.py")
    print("   2. Run: python scripts/generate_data.py")
    print("   3. Run: python scripts/load_data.py")
    print("="*60)
    
    return True

if __name__ == "__main__":
    main()