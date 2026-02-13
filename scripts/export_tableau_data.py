import sys
import os
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.dashboard.tableau_exporter import TableauExporter

def main():
    print("\n" + "="*70)
    print("📊 CREDITFLOW360 - TABLEAU DATA EXPORT")
    print("="*70)
    
    exporter = TableauExporter()
    
    if exporter.export_all():
        print("\n" + "="*70)
        print("✅ DASHBOARD DATA EXPORT COMPLETE!")
        print("="*70)
        print(f"\n📁 Exported files location:")
        print(f"   {exporter.export_dir.absolute()}")
        print(f"\n📋 Files created:")
        for file in exporter.export_dir.glob("*.csv"):
            size = file.stat().st_size / 1024
            print(f"   • {file.name} ({size:.1f} KB)")
        print("\n🎯 Next Step: Open Tableau Public and connect to these CSV files")
        print("="*70)
    else:
        print("\n❌ Export failed. Check the error messages above.")
        sys.exit(1)

if __name__ == "__main__":
    main()