import sys
import os
from pathlib import Path
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.analytics.executive_dashboard import ExecutiveCommandCenter
from src.analytics.credit_risk_monitor import CreditRiskMonitor
from src.analytics.fraud_detection_center import FraudDetectionCenter
from src.analytics.regulatory_reporting import RegulatoryReporting

def run_all():
    print("\n" + "="*70)
    print("🚀 CREDITFLOW360 - MASTER ANALYTICS REPORT GENERATOR")
    print("="*70)
    print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    # 1. Executive Dashboard
    print("\n" + "-"*70)
    print("🏦 EXECUTIVE COMMAND CENTER")
    print("-"*70)
    exec_dash = ExecutiveCommandCenter()
    exec_results = exec_dash.generate_executive_dashboard()
    
    # 2. Credit Risk Monitor
    print("\n" + "-"*70)
    print("📈 CREDIT RISK MONITOR")
    print("-"*70)
    risk_monitor = CreditRiskMonitor()
    risk_results = risk_monitor.generate_credit_risk_report()
    
    # 3. Fraud Detection Center
    print("\n" + "-"*70)
    print("🚨 FRAUD DETECTION CENTER")
    print("-"*70)
    fraud_center = FraudDetectionCenter()
    fraud_results = fraud_center.generate_fraud_report()
    
    # 4. Regulatory Reporting
    print("\n" + "-"*70)
    print("📋 REGULATORY REPORTING")
    print("-"*70)
    regulatory = RegulatoryReporting()
    reg_results = regulatory.generate_regulatory_report()
    
    # Summary
    print("\n" + "="*70)
    print("✅ ALL ANALYTICS REPORTS GENERATED SUCCESSFULLY!")
    print("="*70)
    print(f"\n📁 Report Locations:")
    print(f"   • Executive Dashboard: {exec_dash.report_dir}")
    print(f"   • Credit Risk Monitor: {risk_monitor.report_dir}")
    print(f"   • Fraud Detection Center: {fraud_center.report_dir}")
    print(f"   • Regulatory Reporting: {regulatory.report_dir}")
    print("\n" + "="*70)


if __name__ == "__main__":
    run_all()