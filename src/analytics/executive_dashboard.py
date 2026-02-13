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

class ExecutiveCommandCenter:
    
    def __init__(self):
        self.db = DatabaseConnection()
        self.chart_utils = ChartUtils()
        self.report_gen = ReportGenerator()
        self.chart_utils.set_style()
        
        self.report_dir = Path('src/analytics/reports/executive')
        self.report_dir.mkdir(parents=True, exist_ok=True)

    def get_portfolio_kpis(self):
        logger.info("📊 Calculating portfolio KPIs...")
        
        queries = {
            'active_loans': "SELECT COUNT(*) as value FROM fact_loan WHERE loan_status IN ('Active', 'Overdue')",
            'aum': "SELECT SUM(current_balance) as value FROM fact_loan WHERE loan_status IN ('Active', 'Overdue')",
            'gross_npa': "SELECT SUM(current_balance) as value FROM fact_loan WHERE npa_flag = 1",
            'total_outstanding': "SELECT SUM(current_balance) as value FROM fact_loan",
            'stressed_assets': "SELECT SUM(current_balance) as value FROM fact_loan WHERE days_past_due > 30",
            'avg_yield': "SELECT AVG(interest_rate) as value FROM fact_loan WHERE loan_status IN ('Active', 'Overdue')",
            'total_customers': "SELECT COUNT(*) as value FROM dim_customer WHERE is_active = 1",
            'fraud_alerts_30d': """
                SELECT COUNT(*) as value FROM fact_fraud_alert 
                WHERE detection_date_sk >= (
                    SELECT date_sk FROM dim_date 
                    WHERE full_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) 
                    LIMIT 1
                )
            """
        }
        
        results = {}
        for key, query in queries.items():
            try:
                df = self.db.query_to_dataframe(query)
                results[key] = df['value'].iloc[0] if not df.empty and 'value' in df.columns else 0
            except Exception as e:
                logger.warning(f"⚠️  Error fetching {key}: {e}")
                results[key] = 0
        
        results['gnpa_ratio'] = (results['gross_npa'] / results['total_outstanding'] * 100) if results['total_outstanding'] > 0 else 0
        results['net_npa'] = results['gross_npa'] * 0.3  # Assuming 30% provision
        results['provision_coverage'] = (results['net_npa'] / results['gross_npa'] * 100) if results['gross_npa'] > 0 else 0
        results['credit_cost'] = results['gnpa_ratio'] * 0.4  # Estimated credit cost
        
        logger.info(f"✅ Portfolio KPIs calculated")
        return results
    

    
    def get_portfolio_trends(self, months=12):
        logger.info(f"📈 Getting portfolio trends for last {months} months...")
        
        query = f"""
        SELECT 
            d.year,
            d.month,
            d.month_name,
            COUNT(DISTINCT l.loan_id) as new_loans,
            SUM(l.loan_amount) as disbursements,
            SUM(l.current_balance) as outstanding,
            SUM(CASE WHEN l.npa_flag = 1 THEN l.current_balance ELSE 0 END) as npa_amount,
            AVG(l.interest_rate) as avg_rate
        FROM fact_loan l
        JOIN dim_date d ON l.disbursement_date_sk = d.date_sk
        WHERE l.disbursement_date_sk IS NOT NULL
        AND d.full_date >= DATE_SUB(CURDATE(), INTERVAL {months} MONTH)
        GROUP BY d.year, d.month, d.month_name
        ORDER BY d.year, d.month
        """
        
        df = self.db.query_to_dataframe(query)
        logger.info(f"✅ Got {len(df)} months of trend data")
        return df
    
    def get_product_heatmap(self):
        logger.info("🔥 Creating portfolio heatmap...")
        
        query = """
        SELECT 
            p.product_type,
            c.credit_tier,
            COUNT(DISTINCT l.loan_id) as loan_count,
            SUM(l.current_balance) as exposure,
            AVG(l.probability_of_default) as avg_pd,
            SUM(CASE WHEN l.npa_flag = 1 THEN 1 ELSE 0 END) / COUNT(DISTINCT l.loan_id) * 100 as npa_rate,
            SUM(l.expected_loss) as expected_loss
        FROM fact_loan l
        JOIN dim_product p ON l.product_sk = p.product_sk
        JOIN dim_customer c ON l.customer_sk = c.customer_sk
        WHERE l.loan_status IN ('Active', 'Overdue', 'NPA')
        GROUP BY p.product_type, c.credit_tier
        """
        
        df = self.db.query_to_dataframe(query)
        
        heatmap_data = df.pivot_table(
            values='npa_rate',
            index='product_type',
            columns='credit_tier',
            aggfunc='mean'
        ).fillna(0)
        
        logger.info(f"✅ Heatmap created with shape {heatmap_data.shape}")
        return heatmap_data, df
    
    def get_geographic_distribution(self):
        query = """
        SELECT 
            c.state,
            COUNT(DISTINCT l.loan_id) as loan_count,
            SUM(l.current_balance) as exposure,
            AVG(l.interest_rate) as avg_rate,
            SUM(CASE WHEN l.npa_flag = 1 THEN 1 ELSE 0 END) as npa_count,
            SUM(CASE WHEN l.npa_flag = 1 THEN l.current_balance ELSE 0 END) / 
                NULLIF(SUM(l.current_balance), 0) * 100 as state_npa
        FROM fact_loan l
        JOIN dim_customer c ON l.customer_sk = c.customer_sk
        WHERE l.loan_status IN ('Active', 'Overdue', 'NPA')
        GROUP BY c.state
        ORDER BY exposure DESC
        """
        
        df = self.db.query_to_dataframe(query)
        logger.info(f"✅ Got geographic data for {len(df)} states")
        return df
    
    def generate_executive_dashboard(self):
        logger.info("="*60)
        logger.info("🚀 GENERATING EXECUTIVE COMMAND CENTER")
        logger.info("="*60)
        
        kpis = self.get_portfolio_kpis()
        
        trends = self.get_portfolio_trends()
        
        heatmap_data, heatmap_detail = self.get_product_heatmap()
        
        geo_data = self.get_geographic_distribution()
        
        fig1 = self.chart_utils.create_line_chart(
            trends, 'month_name', 'outstanding', 
            'Portfolio Growth Trend (AUM)',
            'Month', 'Outstanding (₹)',
            markers=True
        )
        self.chart_utils.format_currency(fig1.axes[0])
        self.chart_utils.save_chart(fig1, 'portfolio_trend.png', 'executive')
        
        fig2 = self.chart_utils.create_bar_chart(
            trends, 'month_name', 'disbursements',
            'Monthly Disbursements',
            'Month', 'Disbursement Amount (₹)'
        )
        self.chart_utils.format_currency(fig2.axes[0])
        self.chart_utils.save_chart(fig2, 'monthly_disbursements.png', 'executive')
        
        fig3 = self.chart_utils.create_heatmap(
            heatmap_data,
            'Portfolio Risk Heatmap - NPA % by Product & Credit Tier',
            'Credit Tier', 'Product Type'
        )
        self.chart_utils.save_chart(fig3, 'risk_heatmap.png', 'executive')
        
        top_states = geo_data.head(10)
        fig4 = self.chart_utils.create_bar_chart(
            top_states, 'state', 'exposure',
            'Top 10 States by Portfolio Exposure',
            'State', 'Exposure (₹)',
            horizontal=True
        )
        self.chart_utils.format_currency(fig4.axes[0])
        self.chart_utils.save_chart(fig4, 'top_states.png', 'executive')
        
        product_mix = heatmap_detail.groupby('product_type')['exposure'].sum().reset_index()
        fig5 = self.chart_utils.create_pie_chart(
            product_mix['exposure'].values,
            product_mix['product_type'].values,
            'Portfolio Composition by Product Type'
        )
        self.chart_utils.save_chart(fig5, 'product_mix.png', 'executive')
        
        kpi_df = pd.DataFrame([kpis])
        
        excel_file = self.report_dir / f'executive_dashboard_{datetime.now().strftime("%Y%m%d")}.xlsx'
        self.report_gen.generate_excel_report(
            [kpi_df, trends, heatmap_detail, geo_data],
            ['KPIs', 'Monthly Trends', 'Product Risk', 'Geographic'],
            excel_file
        )
        
        pdf_file = self.report_dir / f'executive_summary_{datetime.now().strftime("%Y%m%d")}.pdf'
        self.report_gen.generate_pdf_report(
            [kpi_df.round(2), trends.head(10).round(2), heatmap_detail.head(10).round(2)],
            ['Key Performance Indicators', 'Recent Trends (Last 10 months)', 'Top Risk Segments'],
            pdf_file
        )
        
        logger.info("\n" + "="*60)
        logger.info("📊 EXECUTIVE SUMMARY")
        logger.info("="*60)
        logger.info(f"🏦 AUM: ₹{kpis['aum']:,.0f}")
        logger.info(f"📈 Active Loans: {kpis['active_loans']:,.0f}")
        logger.info(f"⚠️  GNPA Ratio: {kpis['gnpa_ratio']:.2f}%")
        logger.info(f"👥 Total Customers: {kpis['total_customers']:,.0f}")
        logger.info(f"🚨 Fraud Alerts (30d): {kpis['fraud_alerts_30d']}")
        logger.info("="*60)
        logger.info(f"✅ Executive dashboard generated in: {self.report_dir}")
        
        return {
            'kpis': kpis,
            'trends': trends,
            'heatmap': heatmap_data,
            'geo_data': geo_data,
            'report_files': {
                'excel': excel_file,
                'pdf': pdf_file,
                'charts': list(self.report_dir.glob('*.png'))
            }
        }


if __name__ == "__main__":
    exec_dash = ExecutiveCommandCenter()
    results = exec_dash.generate_executive_dashboard()
    
    print("\n✅ Executive Command Center Generated!")
    print(f"📁 Reports saved to: {exec_dash.report_dir}")