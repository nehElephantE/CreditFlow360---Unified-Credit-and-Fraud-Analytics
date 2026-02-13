import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from pathlib import Path
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.database.db_connection import DatabaseConnection
from src.analytics.utils.chart_utils import ChartUtils
from src.analytics.utils.report_utils import ReportGenerator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FraudDetectionCenter:
    def __init__(self):
        self.db = DatabaseConnection()
        self.chart_utils = ChartUtils()
        self.report_gen = ReportGenerator()
        self.chart_utils.set_style()
        
        self.report_dir = Path('src/analytics/reports/fraud')
        self.report_dir.mkdir(parents=True, exist_ok=True)
    
    def get_fraud_summary(self):
        logger.info("📊 Calculating fraud summary...")
        
        query = """
        SELECT 
            COUNT(*) as total_alerts,
            COUNT(CASE WHEN investigation_status = 'New' THEN 1 END) as new_alerts,
            COUNT(CASE WHEN investigation_status = 'In Progress' THEN 1 END) as in_progress,
            COUNT(CASE WHEN investigation_status = 'Confirmed' THEN 1 END) as confirmed_fraud,
            COUNT(CASE WHEN investigation_status = 'False Positive' THEN 1 END) as false_positives,
            SUM(CASE WHEN investigation_status = 'Confirmed' THEN financial_impact ELSE 0 END) as total_impact,
            AVG(risk_score) as avg_risk_score
        FROM fact_fraud_alert
        """
        
        df = self.db.query_to_dataframe(query)
        
        if len(df) > 0:
            df['confirmation_rate'] = df['confirmed_fraud'] / df['total_alerts'] * 100
            df['false_positive_rate'] = df['false_positives'] / df['total_alerts'] * 100
        
        logger.info(f"✅ Fraud summary calculated")
        return df.iloc[0].to_dict() if not df.empty else {}
    
    def get_fraud_trends(self, days=90):
        logger.info(f"📈 Getting fraud trends for last {days} days...")
        
        query = f"""
        SELECT 
            d.full_date,
            d.year,
            d.month,
            d.day,
            fa.alert_type,
            fa.risk_level,
            COUNT(*) as alert_count,
            SUM(fa.financial_impact) as daily_impact
        FROM fact_fraud_alert fa
        JOIN dim_date d ON fa.detection_date_sk = d.date_sk
        WHERE d.full_date >= DATE_SUB(CURDATE(), INTERVAL {days} DAY)
        GROUP BY d.full_date, d.year, d.month, d.day, fa.alert_type, fa.risk_level
        ORDER BY d.full_date
        """
        
        df = self.db.query_to_dataframe(query)
        logger.info(f"✅ Got {len(df)} fraud trend records")
        return df
    
    def get_fraud_by_type(self):
        query = """
        SELECT 
            alert_type,
            COUNT(*) as alert_count,
            SUM(financial_impact) as total_impact,
            AVG(risk_score) as avg_risk,
            COUNT(CASE WHEN investigation_status = 'Confirmed' THEN 1 END) / 
                NULLIF(COUNT(*), 0) * 100 as confirmation_rate
        FROM fact_fraud_alert
        GROUP BY alert_type
        ORDER BY alert_count DESC
        """
        
        df = self.db.query_to_dataframe(query)
        logger.info(f"✅ Got fraud by type for {len(df)} categories")
        return df
    
    def get_rule_performance(self):
        query = """
        SELECT 
            rule_triggered,
            alert_type,
            COUNT(*) as times_triggered,
            COUNT(CASE WHEN investigation_status = 'Confirmed' THEN 1 END) as confirmed_cases,
            COUNT(CASE WHEN investigation_status = 'False Positive' THEN 1 END) as false_positives,
            SUM(CASE WHEN investigation_status = 'Confirmed' THEN financial_impact ELSE 0 END) as impact_prevented,
            AVG(risk_score) as avg_risk_score
        FROM fact_fraud_alert
        WHERE rule_triggered IS NOT NULL
        GROUP BY rule_triggered, alert_type
        ORDER BY times_triggered DESC
        """
        
        df = self.db.query_to_dataframe(query)
        
        if not df.empty:
            df['precision'] = df['confirmed_cases'] / df['times_triggered'] * 100
            df['false_positive_rate'] = df['false_positives'] / df['times_triggered'] * 100
            df['avg_impact_per_case'] = df['impact_prevented'] / df['confirmed_cases']
        
        logger.info(f"✅ Got rule performance for {len(df)} rules")
        return df
    
    def get_high_risk_customers(self, limit=20):
        query = f"""
        SELECT 
            c.customer_id,
            c.first_name,
            c.last_name,
            c.credit_tier,
            c.annual_income,
            COUNT(fa.alert_id) as fraud_alerts,
            MAX(fa.risk_score) as max_risk_score,
            SUM(CASE WHEN fa.investigation_status = 'Confirmed' THEN 1 ELSE 0 END) as confirmed_frauds,
            SUM(fa.financial_impact) as total_impact
        FROM dim_customer c
        JOIN fact_fraud_alert fa ON c.customer_sk = fa.customer_sk
        GROUP BY c.customer_id, c.first_name, c.last_name, c.credit_tier, c.annual_income
        ORDER BY max_risk_score DESC, fraud_alerts DESC
        LIMIT {limit}
        """
        
        df = self.db.query_to_dataframe(query)
        logger.info(f"✅ Got {len(df)} high-risk customers")
        return df
    
    def get_recent_alerts(self, days=7):
        query = f"""
        SELECT 
            fa.alert_id,
            fa.alert_type,
            fa.risk_score,
            fa.risk_level,
            fa.rule_triggered,
            fa.alert_description,
            fa.investigation_status,
            fa.financial_impact,
            d.full_date as detection_date,
            c.customer_id,
            CONCAT(c.first_name, ' ', c.last_name) as customer_name
        FROM fact_fraud_alert fa
        JOIN dim_date d ON fa.detection_date_sk = d.date_sk
        JOIN dim_customer c ON fa.customer_sk = c.customer_sk
        WHERE d.full_date >= DATE_SUB(CURDATE(), INTERVAL {days} DAY)
        ORDER BY d.full_date DESC, fa.risk_score DESC
        """
        
        df = self.db.query_to_dataframe(query)
        logger.info(f"✅ Got {len(df)} recent fraud alerts")
        return df
    
    def generate_fraud_report(self):
        logger.info("="*60)
        logger.info("🚀 GENERATING FRAUD DETECTION CENTER REPORT")
        logger.info("="*60)
        
        # 1. Get all fraud data
        fraud_summary = self.get_fraud_summary()
        fraud_trends = self.get_fraud_trends()
        fraud_by_type = self.get_fraud_by_type()
        rule_performance = self.get_rule_performance()
        high_risk_customers = self.get_high_risk_customers()
        recent_alerts = self.get_recent_alerts()
        
        if not fraud_trends.empty:
            daily_totals = fraud_trends.groupby('full_date')['alert_count'].sum().reset_index()
            fig1 = self.chart_utils.create_line_chart(
                daily_totals, 'full_date', 'alert_count',
                'Daily Fraud Alerts Trend',
                'Date', 'Number of Alerts',
                markers=True
            )
            self.chart_utils.save_chart(fig1, 'fraud_trends.png', 'fraud')
        
        if not fraud_by_type.empty:
            fig2 = self.chart_utils.create_bar_chart(
                fraud_by_type, 'alert_type', 'alert_count',
                'Fraud Alerts by Type',
                'Fraud Type', 'Number of Alerts',
                horizontal=True
            )
            self.chart_utils.save_chart(fig2, 'fraud_by_type.png', 'fraud')
        
        if not rule_performance.empty:
            top_rules = rule_performance.head(10)
            fig3 = self.chart_utils.create_bar_chart(
                top_rules, 'rule_triggered', 'times_triggered',
                'Top 10 Fraud Detection Rules',
                'Rule', 'Times Triggered',
                horizontal=True
            )
            self.chart_utils.save_chart(fig3, 'top_rules.png', 'fraud')
        
        if not fraud_trends.empty:
            risk_dist = fraud_trends.groupby('risk_level')['alert_count'].sum().reset_index()
            fig4 = self.chart_utils.create_pie_chart(
                risk_dist['alert_count'].values,
                risk_dist['risk_level'].values,
                'Fraud Alerts by Risk Level'
            )
            self.chart_utils.save_chart(fig4, 'risk_distribution.png', 'fraud')
        
        if not fraud_by_type.empty:
            fig5 = self.chart_utils.create_bar_chart(
                fraud_by_type, 'alert_type', 'total_impact',
                'Financial Impact by Fraud Type',
                'Fraud Type', 'Impact (₹)',
                horizontal=True
            )
            self.chart_utils.format_currency(fig5.axes[0])
            self.chart_utils.save_chart(fig5, 'impact_by_type.png', 'fraud')
        
        excel_file = self.report_dir / f'fraud_report_{datetime.now().strftime("%Y%m%d")}.xlsx'
        self.report_gen.generate_excel_report(
            [
                pd.DataFrame([fraud_summary]) if fraud_summary else pd.DataFrame(),
                fraud_by_type,
                rule_performance,
                high_risk_customers,
                recent_alerts
            ],
            ['Summary', 'Fraud_by_Type', 'Rule_Performance', 'High_Risk_Customers', 'Recent_Alerts'],
            excel_file
        )
        
        pdf_file = self.report_dir / f'fraud_summary_{datetime.now().strftime("%Y%m%d")}.pdf'
        self.report_gen.generate_pdf_report(
            [
                pd.DataFrame([fraud_summary]) if fraud_summary else pd.DataFrame(),
                fraud_by_type.head(10),
                rule_performance.head(10)
            ],
            ['Fraud Summary', 'Top Fraud Types', 'Top Performing Rules'],
            pdf_file
        )
        
        logger.info("\n" + "="*60)
        logger.info("🚨 FRAUD DETECTION SUMMARY")
        logger.info("="*60)
        if fraud_summary:
            logger.info(f"📊 Total Alerts: {fraud_summary.get('total_alerts', 0)}")
            logger.info(f"🆕 New Alerts: {fraud_summary.get('new_alerts', 0)}")
            logger.info(f"✅ Confirmed Fraud: {fraud_summary.get('confirmed_fraud', 0)}")
            logger.info(f"❌ False Positives: {fraud_summary.get('false_positives', 0)}")
            logger.info(f"💰 Total Financial Impact: ₹{fraud_summary.get('total_impact', 0):,.0f}")
            logger.info(f"📈 Confirmation Rate: {fraud_summary.get('confirmation_rate', 0):.1f}%")
        logger.info("="*60)
        logger.info(f"✅ Fraud report generated in: {self.report_dir}")
        
        return {
            'summary': fraud_summary,
            'by_type': fraud_by_type,
            'rule_performance': rule_performance,
            'high_risk_customers': high_risk_customers,
            'recent_alerts': recent_alerts,
            'report_files': {
                'excel': excel_file,
                'pdf': pdf_file,
                'charts': list(self.report_dir.glob('*.png'))
            }
        }


if __name__ == "__main__":
    fraud_center = FraudDetectionCenter()
    results = fraud_center.generate_fraud_report()
    
    print("\n✅ Fraud Detection Center Generated!")
    print(f"📁 Reports saved to: {fraud_center.report_dir}")