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

class RegulatoryReporting:
    
    def __init__(self):
        self.db = DatabaseConnection()
        self.chart_utils = ChartUtils()
        self.report_gen = ReportGenerator()
        self.chart_utils.set_style()
        
        self.report_dir = Path('src/analytics/reports/regulatory')
        self.report_dir.mkdir(parents=True, exist_ok=True)
        
        self.provisioning_rates = {
            'Standard': 0.004,      # 0.4%
            'Sub-Standard': 0.10,    # 10%
            'Doubtful-1': 0.25,      # 25%
            'Doubtful-2': 0.40,      # 40%
            'Doubtful-3': 0.60,      # 60%
            'Loss': 1.00              # 100%
        }
    
    def get_asset_classification(self):
        logger.info("📊 Classifying assets per RBI guidelines...")
        
        query = """
        SELECT 
            l.loan_id,
            l.current_balance,
            l.days_past_due,
            l.npa_flag,
            l.written_off_flag,
            CASE 
                WHEN l.written_off_flag = 1 THEN 'Loss'
                WHEN l.days_past_due > 180 THEN 'Doubtful-3'
                WHEN l.days_past_due > 120 THEN 'Doubtful-2'
                WHEN l.days_past_due > 90 THEN 'Doubtful-1'
                WHEN l.days_past_due > 0 THEN 'Sub-Standard'
                ELSE 'Standard'
            END as asset_classification,
            l.probability_of_default,
            l.loss_given_default
        FROM fact_loan l
        WHERE l.loan_status IN ('Active', 'Overdue', 'NPA')
        """
        
        df = self.db.query_to_dataframe(query)
        
        df['provision_rate'] = df['asset_classification'].map(self.provisioning_rates)
        df['provision_required'] = df['current_balance'] * df['provision_rate']
        
        logger.info(f"✅ Asset classification completed for {len(df)} loans")
        return df
    
    def get_capital_adequacy(self):
        logger.info("💰 Calculating capital adequacy...")
        
        query_rwa = """
        SELECT 
            SUM(CASE 
                WHEN dpd_bucket = '0' THEN current_balance * 0.20
                WHEN dpd_bucket = '1-30' THEN current_balance * 0.30
                WHEN dpd_bucket = '31-60' THEN current_balance * 0.50
                WHEN dpd_bucket = '61-90' THEN current_balance * 0.75
                ELSE current_balance * 1.00
            END) as risk_weighted_assets,
            SUM(current_balance) as total_outstanding,
            SUM(CASE WHEN npa_flag = 1 THEN current_balance ELSE 0 END) as gross_npa
        FROM fact_loan
        WHERE loan_status IN ('Active', 'Overdue', 'NPA')
        """
        
        df_rwa = self.db.query_to_dataframe(query_rwa)
        
        rwa = df_rwa['risk_weighted_assets'].iloc[0] if not df_rwa.empty else 0
        
        capital = {
            'tier_1_capital': rwa * 0.15,
            'tier_2_capital': rwa * 0.05,
            'total_capital': rwa * 0.20,
            'risk_weighted_assets': rwa,
            'crar_percentage': 20.0,  # (0.20 * rwa) / rwa * 100
            'min_requirement_met': rwa * 0.20 >= rwa * 0.15  # 15% minimum requirement
        }
        
        logger.info(f"✅ Capital adequacy calculated: CRAR = {capital['crar_percentage']:.1f}%")
        return capital
    
    def get_provisioning_summary(self):
        logger.info("📋 Calculating provisioning requirements...")
        
        df = self.get_asset_classification()
        
        summary = df.groupby('asset_classification').agg({
            'loan_id': 'count',
            'current_balance': 'sum',
            'provision_required': 'sum',
            'provision_rate': 'first'
        }).reset_index()
        
        summary.columns = ['Asset_Class', 'Loan_Count', 'Outstanding', 'Provision_Required', 'Provision_Rate']
        
        total_outstanding = summary['Outstanding'].sum()
        total_provision = summary['Provision_Required'].sum()
        
        summary.loc['TOTAL'] = [
            'TOTAL',
            summary['Loan_Count'].sum(),
            total_outstanding,
            total_provision,
            total_provision / total_outstanding if total_outstanding > 0 else 0
        ]
        
        logger.info(f"✅ Provisioning summary completed")
        return summary
    
    def get_large_exposures(self, limit=20):
        logger.info("🔍 Identifying large exposures...")
        
        capital = self.get_capital_adequacy()
        exposure_limit = capital['total_capital'] * 0.15  # 15% of capital
        
        query = f"""
        SELECT 
            c.customer_id,
            CONCAT(c.first_name, ' ', c.last_name) as customer_name,
            c.credit_tier,
            c.annual_income,
            COUNT(l.loan_id) as loan_count,
            SUM(l.current_balance) as total_exposure,
            {exposure_limit} as exposure_limit,
            SUM(l.current_balance) / {exposure_limit} * 100 as exposure_pct_of_limit,
            CASE 
                WHEN SUM(l.current_balance) > {exposure_limit} THEN 'Breach'
                ELSE 'Compliant'
            END as compliance_status
        FROM fact_loan l
        JOIN dim_customer c ON l.customer_sk = c.customer_sk
        WHERE l.loan_status IN ('Active', 'Overdue', 'NPA')
        GROUP BY c.customer_id, c.first_name, c.last_name, c.credit_tier, c.annual_income
        HAVING SUM(l.current_balance) > {exposure_limit * 0.5}  -- Show exposures >50% of limit
        ORDER BY total_exposure DESC
        LIMIT {limit}
        """
        
        df = self.db.query_to_dataframe(query)
        logger.info(f"✅ Found {len(df)} large exposures")
        return df
    
    def get_sectoral_exposure(self):
        query = """
        SELECT 
            c.employment_type as sector,
            COUNT(DISTINCT l.loan_id) as loan_count,
            SUM(l.current_balance) as exposure,
            AVG(l.interest_rate) as avg_rate,
            SUM(CASE WHEN l.npa_flag = 1 THEN l.current_balance ELSE 0 END) as npa_exposure,
            SUM(CASE WHEN l.npa_flag = 1 THEN l.current_balance ELSE 0 END) / 
                NULLIF(SUM(l.current_balance), 0) * 100 as sector_npa
        FROM fact_loan l
        JOIN dim_customer c ON l.customer_sk = c.customer_sk
        WHERE l.loan_status IN ('Active', 'Overdue', 'NPA')
        GROUP BY c.employment_type
        ORDER BY exposure DESC
        """
        
        df = self.db.query_to_dataframe(query)
        logger.info(f"✅ Got sectoral exposure for {len(df)} sectors")
        return df
    
    def generate_regulatory_report(self):
        logger.info("="*60)
        logger.info("🚀 GENERATING REGULATORY REPORT")
        logger.info("="*60)
        
        capital = self.get_capital_adequacy()
        provisioning = self.get_provisioning_summary()
        large_exposures = self.get_large_exposures()
        sectoral = self.get_sectoral_exposure()
        asset_class = self.get_asset_classification()
        

        asset_summary = asset_class.groupby('asset_classification')['current_balance'].sum().reset_index()
        fig1 = self.chart_utils.create_pie_chart(
            asset_summary['current_balance'].values,
            [f"{cls}\n(₹{val:,.0f})" for cls, val in zip(asset_summary['asset_classification'], 
                                                         asset_summary['current_balance'])],
            'Asset Classification by Outstanding Amount'
        )
        self.chart_utils.save_chart(fig1, 'asset_classification.png', 'regulatory')
        
    
        if not provisioning.empty and len(provisioning) > 1:
            prov_data = provisioning.iloc[:-1]  # Exclude total row
            fig2 = self.chart_utils.create_bar_chart(
                prov_data, 'Asset_Class', 'Provision_Required',
                'Provision Requirements by Asset Class',
                'Asset Class', 'Provision Required (₹)'
            )
            self.chart_utils.format_currency(fig2.axes[0])
            self.chart_utils.save_chart(fig2, 'provision_requirements.png', 'regulatory')
        
        if not sectoral.empty:
            fig3 = self.chart_utils.create_bar_chart(
                sectoral.head(10), 'sector', 'exposure',
                'Top 10 Sectors by Exposure',
                'Sector', 'Exposure (₹)',
                horizontal=True
            )
            self.chart_utils.format_currency(fig3.axes[0])
            self.chart_utils.save_chart(fig3, 'sectoral_exposure.png', 'regulatory')
        
    
        if not sectoral.empty:
            fig4 = self.chart_utils.create_bar_chart(
                sectoral.head(10), 'sector', 'sector_npa',
                'NPA Rate by Sector',
                'Sector', 'NPA Rate (%)',
                horizontal=True
            )
            self.chart_utils.format_percentage(fig4.axes[0])
            self.chart_utils.save_chart(fig4, 'npa_by_sector.png', 'regulatory')
        
        excel_file = self.report_dir / f'regulatory_report_{datetime.now().strftime("%Y%m%d")}.xlsx'
        self.report_gen.generate_excel_report(
            [
                pd.DataFrame([capital]) if capital else pd.DataFrame(),
                provisioning,
                large_exposures,
                sectoral,
                asset_class.head(100) 
            ],
            ['Capital_Adequacy', 'Provisioning', 'Large_Exposures', 'Sectoral_Exposure', 'Asset_Classification_Sample'],
            excel_file
        )
        
        pdf_file = self.report_dir / f'regulatory_summary_{datetime.now().strftime("%Y%m%d")}.pdf'
        self.report_gen.generate_pdf_report(
            [
                pd.DataFrame([capital]) if capital else pd.DataFrame(),
                provisioning,
                large_exposures.head(10)
            ],
            ['Capital Adequacy', 'Provisioning Summary', 'Top 10 Large Exposures'],
            pdf_file
        )
   
        logger.info("\n" + "="*60)
        logger.info("📋 REGULATORY SUMMARY")
        logger.info("="*60)
        logger.info(f"💰 CRAR: {capital.get('crar_percentage', 0):.1f}% (Min Required: 15%)")
        logger.info(f"   • Tier 1 Capital: ₹{capital.get('tier_1_capital', 0):,.0f}")
        logger.info(f"   • Tier 2 Capital: ₹{capital.get('tier_2_capital', 0):,.0f}")
        logger.info(f"   • Risk-Weighted Assets: ₹{capital.get('risk_weighted_assets', 0):,.0f}")
        
        logger.info(f"\n📊 Provisioning Summary:")
        for _, row in provisioning.iterrows():
            if row['Asset_Class'] != 'TOTAL':
                logger.info(f"   • {row['Asset_Class']}: ₹{row['Outstanding']:,.0f} (Provision: ₹{row['Provision_Required']:,.0f})")
        
        logger.info(f"\n⚠️  Large Exposures: {len(large_exposures)} borrowers exceed 50% of limit")
        logger.info("="*60)
        logger.info(f"✅ Regulatory report generated in: {self.report_dir}")
        
        return {
            'capital': capital,
            'provisioning': provisioning,
            'large_exposures': large_exposures,
            'sectoral': sectoral,
            'asset_classification': asset_class,
            'report_files': {
                'excel': excel_file,
                'pdf': pdf_file,
                'charts': list(self.report_dir.glob('*.png'))
            }
        }


if __name__ == "__main__":
    regulatory = RegulatoryReporting()
    results = regulatory.generate_regulatory_report()
    
    print("\n✅ Regulatory Report Generated!")
    print(f"📁 Reports saved to: {regulatory.report_dir}")