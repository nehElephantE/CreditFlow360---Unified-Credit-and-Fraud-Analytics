import sys
import os
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.database.db_connection import DatabaseConnection

def main():
    print("\n" + "="*60)
    print("🚀 CREDITFLOW360 - SCHEMA CREATION")
    print("="*60)
    
    db = DatabaseConnection()
    
    print("\n📁 Step 1: Creating database...")
    if db.create_database():
        print("   ✅ Database created/verified")
    else:
        print("   ❌ Failed to create database")
        return
    
    print("\n📖 Step 2: Reading schema file...")
    schema_path = Path('src/database/schema_creation.sql')
    
    if not schema_path.exists():
        print(f"   ❌ Schema file not found: {schema_path}")
        return
    
    with open(schema_path, 'r', encoding='utf-8') as f:
        schema_sql = f.read()
    print(f"   ✅ Schema file loaded ({len(schema_sql)} characters)")
    
    print("\n⚙️  Step 3: Creating tables...")
    
    statements = schema_sql.split(';')
    success_count = 0
    error_count = 0
    
    for i, statement in enumerate(statements):
        statement = statement.strip()
        if statement and not statement.startswith('--') and not 'DELIMITER' in statement:
            try:
                db.execute_query(statement)
                success_count += 1
                if i % 10 == 0:
                    print(f"   Progress: {i}/{len(statements)} statements executed")
            except Exception as e:
                error_count += 1
                if "already exists" not in str(e).lower():
                    print(f"   ⚠️  Warning on statement {i+1}: {e}")
    
    print(f"\n   ✅ Tables created: {success_count}")
    print(f"   ⚠️  Warnings: {error_count}")
    
    print("\n🔍 Step 4: Verifying...")
    try:
        result = db.query_to_dataframe("SHOW TABLES")
        print(f"   📋 Tables in database ({len(result)}):")
        for _, row in result.iterrows():
            print(f"      • {row.iloc[0]}")
    except Exception as e:
        print(f"   ⚠️  Could not verify tables: {e}")
    
    print("\n" + "="*60)
    print("✅ SCHEMA CREATION COMPLETE!")
    print("="*60)

if __name__ == "__main__":
    main()