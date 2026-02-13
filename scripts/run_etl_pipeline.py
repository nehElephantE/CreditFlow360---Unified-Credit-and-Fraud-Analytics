import sys
import os
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.etl_python.etl_orchestrator import CreditFlowETL
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    print("\n" + "=" * 70)
    print("🚀 CREDITFLOW360 - STEP 3: ETL PIPELINE")
    print("=" * 70)
    print("\nThis script will:")
    print("  1. Load date dimension (2022-2026)")
    print("  2. Load customer dimension (from CSV)")
    print("  3. Load loan fact table (with dimension lookups)")
    print("  4. Load transaction fact table")
    print("  5. Load fraud alert fact table")
    print("  6. Verify data integrity")
    print("=" * 70 + "\n")
    
    response = input("⚠️  This will truncate and reload all tables. Continue? (yes/no): ")
    if response.lower() != 'yes':
        print("❌ ETL pipeline cancelled.")
        return
    
    try:
        etl = CreditFlowETL()
        report = etl.run_all()
        
        print("\n" + "=" * 70)
        print(f"🏁 ETL PIPELINE COMPLETED")
        print("=" * 70)
        print(f"   Status: {report['overall_status']}")
        print(f"   Duration: {report['duration_seconds']:.2f} seconds")
        print(f"   Failed Steps: {report['failed_steps_count']}")
        print("\n📊 Records Loaded:")
        
        for step, result in report['pipeline_results'].items():
            if step not in ['verification'] and isinstance(result, dict):
                print(f"   • {step:20}: {result.get('records_loaded', 0):10,} records")
        
        print("\n📁 ETL Report: data/etl_report_*.json")
        print("=" * 70)
        
    except KeyboardInterrupt:
        print("\n⚠️  ETL pipeline interrupted by user")
    except Exception as e:
        print(f"\n❌ ETL pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()