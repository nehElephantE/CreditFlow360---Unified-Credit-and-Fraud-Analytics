import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
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

class CreditRiskMonitor:
    
    def __init__(self):
        self.db = DatabaseConnection()
        self.chart_utils = ChartUtils()
        self.report_gen = ReportGenerator()
        self.chart_utils.set_style()
        
        self.report_dir = Path('src/analytics/reports/credit_risk')
        self.report_dir.mkdir(parents=True, exist_ok=True)
    
    def get_pd_lgd_analysis(self):
        logger.info("📊 Analyzing PD and LGD distributions...")
        
        query = """
        SELECT 
            c.credit_tier,
            p.product_type,
            COUNT(l.loan_id) as loan_count,
            AVG(l.probability_of_default) as avg_pd,
            AVG(l.loss_given_default) as avg_lgd,
            AVG(l.probability_of_default * l.loss_given_default) as expected_loss_rate,
            SUM(l.expected_loss) as total_expected_loss,
            SUM(l.current_balance) as exposure
        FROM fact_loan l
        JOIN dim_customer c ON l.customer_sk = c.customer_sk
        JOIN dim_product p ON l.product_sk = p.product_sk
        WHERE l.loan_status IN ('Active', 'Overdue', 'NPA')
        GROUP BY c.credit_tier, p.product_type
        ORDER BY exposure DESC
        """
        
        df = self.db.query_to_dataframe(query)
        
        df['risk_weighted_assets'] = df['exposure'] * df['avg_pd'] * df['avg_lgd']
        
        logger.info(f"✅ PD/LGD analysis completed for {len(df)} segments")
        return df
    
    def get_vintage_curves(self):
        logger.info("📈 Generating vintage curves...")
        
        query = """
        SELECT 
            d.year as vintage_year,
            d.quarter as vintage_quarter,
            p.product_type,
            l.loan_id,
            l.loan_amount,
            d.full_date as disbursement_date,
            TIMESTAMPDIFF(MONTH, d.full_date, CURDATE()) as loan_age_months,
            l.days_past_due,
            CASE 
                WHEN l.days_past_due > 90 THEN 1 
                WHEN l.days_past_due > 60 THEN 2
                WHEN l.days_past_due > 30 THEN 3
                WHEN l.days_past_due > 0 THEN 4
                ELSE 0
            END as delinquency_stage,
            l.npa_flag,
            l.written_off_flag
        FROM fact_loan l
        JOIN dim_date d ON l.disbursement_date_sk = d.date_sk
        JOIN dim_product p ON l.product_sk = p.product_sk
        WHERE l.disbursement_date_sk IS NOT NULL
        AND d.full_date >= DATE_SUB(CURDATE(), INTERVAL 3 YEAR)
        """
        
        try:
            df = self.db.query_to_dataframe(query)
            
            if df.empty:
                logger.warning("⚠️ No vintage data available")
                return pd.DataFrame()
            
            vintages = []
            
            for (year, quarter), group in df.groupby(['vintage_year', 'vintage_quarter']):
                vintage_name = f"{year}Q{quarter}"
                
                
                for month in range(1, 37):
                    month_group = group[group['loan_age_months'] >= month]
                    if len(month_group) > 0:
                        delinq_rate = (month_group['days_past_due'] > 30).mean() * 100
                        npa_rate = month_group['npa_flag'].mean() * 100
                        
                        vintages.append({
                            'vintage': vintage_name,
                            'month': month,
                            'delinquency_rate': delinq_rate,
                            'npa_rate': npa_rate,
                            'loan_count': len(month_group)
                        })
            
            vintage_df = pd.DataFrame(vintages)
            logger.info(f"✅ Vintage curves generated with {len(vintage_df)} data points")
            return vintage_df
            
        except Exception as e:
            logger.error(f"❌ Error generating vintage curves: {e}")
            return pd.DataFrame()
    



    def get_credit_quality_distribution(self):
        query = """
        SELECT 
            c.credit_tier,
            COUNT(DISTINCT l.loan_id) as loan_count,
            SUM(l.current_balance) as exposure,
            AVG(l.interest_rate) as avg_rate,
            SUM(l.expected_loss) as expected_loss,
            SUM(CASE WHEN l.npa_flag = 1 THEN l.current_balance ELSE 0 END) as npa_exposure
        FROM fact_loan l
        JOIN dim_customer c ON l.customer_sk = c.customer_sk
        WHERE l.loan_status IN ('Active', 'Overdue', 'NPA')
        GROUP BY c.credit_tier
        ORDER BY 
            CASE c.credit_tier
                WHEN 'Prime' THEN 1
                WHEN 'Near-Prime' THEN 2
                WHEN 'Sub-Prime' THEN 3
                WHEN 'Deep-Subprime' THEN 4
                ELSE 5
            END
        """
        
        df = self.db.query_to_dataframe(query)
        
        total_exposure = df['exposure'].sum()
        df['exposure_pct'] = df['exposure'] / total_exposure * 100
        df['npa_ratio'] = df['npa_exposure'] / df['exposure'] * 100
        
        logger.info(f"✅ Credit quality distribution calculated")
        return df
    
    def get_migration_analysis(self):
        logger.info("⚠️  Migration analysis requires historical data - skipping for now")
        return pd.DataFrame()
    
    def generate_credit_risk_report(self):
        logger.info("="*60)
        logger.info("🚀 GENERATING CREDIT RISK MONITOR")
        logger.info("="*60)
        
        pd_lgd_data = self.get_pd_lgd_analysis()
        
        vintage_data = self.get_vintage_curves()
        
        quality_data = self.get_credit_quality_distribution()
        
        pd_by_tier = quality_data[['credit_tier', 'exposure', 'expected_loss']].copy()
        pd_by_tier['implied_pd'] = pd_by_tier['expected_loss'] / pd_by_tier['exposure'] * 100
        
        fig1 = self.chart_utils.create_bar_chart(
            pd_by_tier, 'credit_tier', 'implied_pd',
            'Probability of Default by Credit Tier',
            'Credit Tier', 'PD (%)'
        )
        self.chart_utils.format_percentage(fig1.axes[0])
        self.chart_utils.save_chart(fig1, 'pd_by_tier.png', 'credit_risk')
        
        lgd_by_product = pd_lgd_data.groupby('product_type')[['avg_lgd', 'exposure']].agg({
            'avg_lgd': 'mean',
            'exposure': 'sum'
        }).reset_index()
        
        fig2 = self.chart_utils.create_bar_chart(
            lgd_by_product, 'product_type', 'avg_lgd',
            'Loss Given Default by Product Type',
            'Product Type', 'LGD (%)'
        )
        self.chart_utils.format_percentage(fig2.axes[0])
        self.chart_utils.save_chart(fig2, 'lgd_by_product.png', 'credit_risk')
        
        if not vintage_data.empty:
            top_vintages = vintage_data.groupby('vintage')['loan_count'].max().nlargest(5).index
            plot_data = vintage_data[vintage_data['vintage'].isin(top_vintages)]
            
            fig3, ax = plt.subplots(figsize=(14, 8))
            for vintage in top_vintages:
                vintage_subset = plot_data[plot_data['vintage'] == vintage]
                ax.plot(vintage_subset['month'], vintage_subset['npa_rate'], 
                       marker='o', linewidth=2, label=vintage)
            
            ax.set_title('Vintage Curves - NPA Rate by Loan Age', fontsize=16, fontweight='bold')
            ax.set_xlabel('Months Since Disbursement')
            ax.set_ylabel('NPA Rate (%)')
            ax.legend(title='Vintage')
            ax.grid(True, alpha=0.3)
            
            self.chart_utils.save_chart(fig3, 'vintage_curves.png', 'credit_risk')
        
        fig4 = self.chart_utils.create_pie_chart(
            quality_data['exposure'].values,
            [f"{tier}\n(₹{exp:,.0f})" for tier, exp in zip(quality_data['credit_tier'], quality_data['exposure'])],
            'Portfolio Distribution by Credit Tier'
        )
        self.chart_utils.save_chart(fig4, 'credit_quality_pie.png', 'credit_risk')
        
        heatmap_data = pd_lgd_data.pivot_table(
            values='expected_loss_rate',
            index='product_type',
            columns='credit_tier',
            aggfunc='mean'
        ).fillna(0)
        
        if not heatmap_data.empty:
            fig5 = self.chart_utils.create_heatmap(
                heatmap_data * 100,  
                'Expected Loss Rate (%) by Product and Credit Tier',
                'Credit Tier', 'Product Type',
                cmap='YlOrRd'
            )
            self.chart_utils.save_chart(fig5, 'expected_loss_heatmap.png', 'credit_risk')
        
        excel_file = self.report_dir / f'credit_risk_{datetime.now().strftime("%Y%m%d")}.xlsx'
        self.report_gen.generate_excel_report(
            [pd_lgd_data, quality_data, vintage_data if not vintage_data.empty else pd.DataFrame()],
            ['PD_LGD_Analysis', 'Credit_Quality', 'Vintage_Curves'],
            excel_file
        )
        
        pdf_file = self.report_dir / f'credit_risk_summary_{datetime.now().strftime("%Y%m%d")}.pdf'
        self.report_gen.generate_pdf_report(
            [pd_lgd_data.head(15).round(4), quality_data.round(2)],
            ['PD/LGD by Segment (Top 15)', 'Credit Quality Distribution'],
            pdf_file
        )
        
        logger.info("\n" + "="*60)
        logger.info("📊 CREDIT RISK SUMMARY")
        logger.info("="*60)
        logger.info(f"📈 Portfolio PD: {pd_lgd_data['avg_pd'].mean()*100:.2f}%")
        logger.info(f"💸 Portfolio LGD: {pd_lgd_data['avg_lgd'].mean()*100:.2f}%")
        logger.info(f"💰 Expected Loss: ₹{pd_lgd_data['total_expected_loss'].sum():,.0f}")
        
        for _, row in quality_data.iterrows():
            logger.info(f"   • {row['credit_tier']}: {row['exposure_pct']:.1f}% of portfolio, NPA: {row['npa_ratio']:.2f}%")
        
        logger.info("="*60)
        logger.info(f"✅ Credit risk report generated in: {self.report_dir}")
        
        return {
            'pd_lgd_data': pd_lgd_data,
            'quality_data': quality_data,
            'vintage_data': vintage_data,
            'report_files': {
                'excel': excel_file,
                'pdf': pdf_file,
                'charts': list(self.report_dir.glob('*.png'))
            }
        }


if __name__ == "__main__":
    risk_monitor = CreditRiskMonitor()
    results = risk_monitor.generate_credit_risk_report()
    
    print("\n✅ Credit Risk Monitor Generated!")
    print(f"📁 Reports saved to: {risk_monitor.report_dir}")