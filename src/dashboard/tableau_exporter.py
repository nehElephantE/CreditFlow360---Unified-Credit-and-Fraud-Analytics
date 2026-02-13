import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import os
from pathlib import Path

from src.database.db_connection import DatabaseConnection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TableauExporter:
    
    def __init__(self):
        self.db = DatabaseConnection()
        self.export_dir = Path('data/exports/tableau')
        self.export_dir.mkdir(parents=True, exist_ok=True)
        
    def export_executive_dashboard(self):
        logger.info("📊 Exporting Executive Dashboard data...")
        
        query_portfolio = """
        SELECT 
            d.year,
            d.month,
            d.month_name,
            p.product_type,
            COUNT(DISTINCT l.loan_id) as loans_originated,
            SUM(l.loan_amount) as disbursement_amount,
            SUM(l.current_balance) as outstanding_amount,
            AVG(l.interest_rate) as avg_interest_rate,
            SUM(CASE WHEN l.npa_flag = 1 THEN 1 ELSE 0 END) as npa_count,
            SUM(CASE WHEN l.npa_flag = 1 THEN l.current_balance ELSE 0 END) as npa_amount
        FROM fact_loan l
        JOIN dim_date d ON l.disbursement_date_sk = d.date_sk
        JOIN dim_product p ON l.product_sk = p.product_sk
        WHERE l.disbursement_date_sk IS NOT NULL
        GROUP BY d.year, d.month, d.month_name, p.product_type
        ORDER BY d.year, d.month
        """
        
        df_portfolio = self.db.query_to_dataframe(query_portfolio)
        df_portfolio.to_csv(self.export_dir / 'executive_portfolio.csv', index=False)
        logger.info(f"   ✅ Exported {len(df_portfolio)} portfolio records")
        
        query_metrics = """
        SELECT 
            (SELECT COUNT(*) FROM dim_customer WHERE is_active = 1) as total_active_customers,
            (SELECT COUNT(*) FROM fact_loan WHERE loan_status IN ('Active', 'Overdue')) as active_loans,
            (SELECT SUM(current_balance) FROM fact_loan WHERE loan_status IN ('Active', 'Overdue')) as total_outstanding,
            (SELECT AVG(interest_rate) FROM fact_loan WHERE loan_status IN ('Active', 'Overdue')) as avg_interest_rate,
            (SELECT SUM(CASE WHEN npa_flag = 1 THEN current_balance ELSE 0 END) FROM fact_loan) as gross_npa_amount,
            (SELECT 
                SUM(CASE WHEN npa_flag = 1 THEN current_balance ELSE 0 END) / 
                NULLIF(SUM(current_balance), 0) * 100 
            FROM fact_loan) as gnpa_ratio,
            (SELECT COUNT(*) FROM fact_fraud_alert 
             WHERE detection_date_sk >= (
                SELECT date_sk FROM dim_date 
                WHERE full_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) 
                LIMIT 1
             )) as fraud_alerts_30d,
            (SELECT SUM(financial_impact) FROM fact_fraud_alert 
             WHERE investigation_status = 'Confirmed') as total_fraud_impact
        """
        
        df_metrics = self.db.query_to_dataframe(query_metrics)
        df_metrics.to_csv(self.export_dir / 'executive_metrics.csv', index=False)
        logger.info(f"   ✅ Exported metrics snapshot")
        
        return True
    
    def export_risk_dashboard(self):
        logger.info("📊 Exporting Risk Dashboard data...")
        
        query_risk_heatmap = """
        SELECT 
            c.credit_tier,
            p.product_type,
            COUNT(DISTINCT l.loan_id) as loan_count,
            SUM(l.current_balance) as exposure,
            AVG(l.probability_of_default) as avg_pd,
            AVG(l.loss_given_default) as avg_lgd,
            SUM(l.current_balance * l.probability_of_default * l.loss_given_default) as expected_loss,
            SUM(CASE WHEN l.days_past_due > 30 THEN 1 ELSE 0 END) / NULLIF(COUNT(DISTINCT l.loan_id), 0) * 100 as delinquency_rate,
            SUM(CASE WHEN l.npa_flag = 1 THEN 1 ELSE 0 END) / NULLIF(COUNT(DISTINCT l.loan_id), 0) * 100 as npa_rate
        FROM fact_loan l
        JOIN dim_customer c ON l.customer_sk = c.customer_sk
        JOIN dim_product p ON l.product_sk = p.product_sk
        WHERE l.loan_status IN ('Active', 'Overdue', 'NPA')
        GROUP BY c.credit_tier, p.product_type
        """
        
        df_heatmap = self.db.query_to_dataframe(query_risk_heatmap)
        df_heatmap.to_csv(self.export_dir / 'risk_heatmap.csv', index=False)
        logger.info(f"   ✅ Exported {len(df_heatmap)} risk heatmap records")
        
        query_vintage = """
        SELECT 
            d.year as vintage_year,
            p.product_type,
            COUNT(DISTINCT l.loan_id) as origination_volume,
            SUM(l.loan_amount) as origination_amount,
            AVG(CASE WHEN DATEDIFF(CURDATE(), d.full_date) BETWEEN 0 AND 30 THEN 1 ELSE 0 END) as current_rate,
            AVG(CASE WHEN l.days_past_due BETWEEN 1 AND 30 THEN 1 ELSE 0 END) as dpd_30_rate,
            AVG(CASE WHEN l.days_past_due BETWEEN 31 AND 60 THEN 1 ELSE 0 END) as dpd_60_rate,
            AVG(CASE WHEN l.days_past_due BETWEEN 61 AND 90 THEN 1 ELSE 0 END) as dpd_90_rate,
            AVG(CASE WHEN l.npa_flag = 1 THEN 1 ELSE 0 END) as npa_rate
        FROM fact_loan l
        JOIN dim_date d ON l.disbursement_date_sk = d.date_sk
        JOIN dim_product p ON l.product_sk = p.product_sk
        WHERE l.disbursement_date_sk IS NOT NULL
        GROUP BY d.year, p.product_type
        ORDER BY vintage_year DESC
        """
        
        df_vintage = self.db.query_to_dataframe(query_vintage)
        df_vintage.to_csv(self.export_dir / 'risk_vintage.csv', index=False)
        logger.info(f"   ✅ Exported {len(df_vintage)} vintage analysis records")
        
        query_dpd = """
        SELECT 
            l.dpd_bucket,
            COUNT(*) as loan_count,
            SUM(l.current_balance) as outstanding_amount,
            AVG(l.days_past_due) as avg_dpd,
            AVG(l.probability_of_default) as avg_pd
        FROM fact_loan l
        WHERE l.loan_status IN ('Active', 'Overdue', 'NPA')
        GROUP BY l.dpd_bucket
        ORDER BY FIELD(l.dpd_bucket, '0', '1-30', '31-60', '61-90', '90+')
        """
        
        df_dpd = self.db.query_to_dataframe(query_dpd)
        df_dpd.to_csv(self.export_dir / 'risk_dpd_distribution.csv', index=False)
        logger.info(f"   ✅ Exported DPD distribution")
        
        return True
    
    def export_fraud_dashboard(self):
        logger.info("📊 Exporting Fraud Dashboard data...")
        
        # 1. Fraud Trends
        query_fraud_trends = """
        SELECT 
            d.year,
            d.month,
            d.month_name,
            fa.alert_type,
            fa.risk_level,
            COUNT(*) as alert_count,
            SUM(fa.financial_impact) as financial_impact,
            COUNT(CASE WHEN fa.investigation_status = 'Confirmed' THEN 1 END) as confirmed_cases,
            AVG(fa.risk_score) as avg_risk_score
        FROM fact_fraud_alert fa
        JOIN dim_date d ON fa.detection_date_sk = d.date_sk
        GROUP BY d.year, d.month, d.month_name, fa.alert_type, fa.risk_level
        ORDER BY d.year, d.month
        """
        
        df_trends = self.db.query_to_dataframe(query_fraud_trends)
        df_trends.to_csv(self.export_dir / 'fraud_trends.csv', index=False)
        logger.info(f"   ✅ Exported {len(df_trends)} fraud trend records")
        
        query_fraud_segment = """
        SELECT 
            c.credit_tier,
            c.income_tier,
            c.customer_segment,
            COUNT(DISTINCT fa.alert_id) as fraud_alerts,
            COUNT(DISTINCT CASE WHEN fa.investigation_status = 'Confirmed' THEN fa.alert_id END) as confirmed_fraud,
            SUM(fa.financial_impact) as total_impact,
            COUNT(DISTINCT l.loan_id) as total_loans,
            COUNT(DISTINCT fa.alert_id) / NULLIF(COUNT(DISTINCT l.loan_id), 0) * 100 as fraud_rate
        FROM dim_customer c
        LEFT JOIN fact_loan l ON c.customer_sk = l.customer_sk
        LEFT JOIN fact_fraud_alert fa ON c.customer_sk = fa.customer_sk
        GROUP BY c.credit_tier, c.income_tier, c.customer_segment
        """
        
        df_segment = self.db.query_to_dataframe(query_fraud_segment)
        df_segment.to_csv(self.export_dir / 'fraud_by_segment.csv', index=False)
        logger.info(f"   ✅ Exported fraud by segment")
        
        query_rules = """
        SELECT 
            rule_triggered,
            alert_type,
            COUNT(*) as trigger_count,
            SUM(financial_impact) as total_impact,
            AVG(risk_score) as avg_risk_score,
            COUNT(CASE WHEN investigation_status = 'Confirmed' THEN 1 END) / NULLIF(COUNT(*), 0) * 100 as accuracy_rate
        FROM fact_fraud_alert
        WHERE rule_triggered IS NOT NULL
        GROUP BY rule_triggered, alert_type
        ORDER BY trigger_count DESC
        LIMIT 20
        """
        
        df_rules = self.db.query_to_dataframe(query_rules)
        df_rules.to_csv(self.export_dir / 'fraud_top_rules.csv', index=False)
        logger.info(f"   ✅ Exported top fraud rules")
        
        return True
    
    def export_collection_dashboard(self):
        logger.info("📊 Exporting Collection Dashboard data...")
        
        # 1. Collection Efficiency
        query_collection = """
        SELECT 
            l.collection_tier,
            COUNT(DISTINCT l.loan_id) as assigned_loans,
            SUM(l.overdue_amount) as total_overdue,
            SUM(t.amount) as amount_collected,
            COUNT(DISTINCT t.transaction_id) as collection_attempts,
            SUM(t.amount) / NULLIF(SUM(l.overdue_amount), 0) * 100 as collection_efficiency,
            AVG(l.days_past_due) as avg_dpd
        FROM fact_loan l
        LEFT JOIN fact_transaction t ON l.loan_sk = t.loan_sk 
            AND t.transaction_type = 'EMI'
            AND t.transaction_date_sk >= (
                SELECT date_sk FROM dim_date 
                WHERE full_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
                LIMIT 1
            )
        WHERE l.days_past_due > 0
        GROUP BY l.collection_tier
        """
        
        df_collection = self.db.query_to_dataframe(query_collection)
        df_collection.to_csv(self.export_dir / 'collection_efficiency.csv', index=False)
        logger.info(f"   ✅ Exported collection efficiency")
        
        # 2. Aging Buckets
        query_aging = """
        SELECT 
            l.dpd_bucket,
            COUNT(*) as loan_count,
            SUM(l.current_balance) as outstanding,
            SUM(l.overdue_amount) as overdue_amount,
            AVG(l.days_past_due) as avg_dpd,
            AVG(l.probability_of_default) as avg_pd,
            SUM(l.overdue_amount * 0.4) as provision_required
        FROM fact_loan l
        WHERE l.days_past_due > 0
        GROUP BY l.dpd_bucket
        ORDER BY FIELD(l.dpd_bucket, '1-30', '31-60', '61-90', '90+')
        """
        
        df_aging = self.db.query_to_dataframe(query_aging)
        df_aging.to_csv(self.export_dir / 'collection_aging.csv', index=False)
        logger.info(f"   ✅ Exported aging buckets")
        
        return True
    
    def export_regulatory_dashboard(self):
        logger.info("📊 Exporting Regulatory Dashboard data...")
        
        # 1. Capital Adequacy
        query_capital = """
        SELECT 
            'Tier 1 Capital' as component,
            SUM(current_balance * 0.15) as amount
        FROM fact_loan
        WHERE loan_status IN ('Active', 'Overdue', 'NPA')
        UNION ALL
        SELECT 'Tier 2 Capital',
            SUM(current_balance * 0.05)
        FROM fact_loan
        WHERE loan_status IN ('Active', 'Overdue', 'NPA')
        UNION ALL
        SELECT 'Risk Weighted Assets',
            SUM(CASE 
                WHEN dpd_bucket = '0' THEN current_balance * 0.20
                WHEN dpd_bucket = '1-30' THEN current_balance * 0.30
                WHEN dpd_bucket = '31-60' THEN current_balance * 0.50
                WHEN dpd_bucket = '61-90' THEN current_balance * 0.75
                ELSE current_balance * 1.00
            END)
        FROM fact_loan
        WHERE loan_status IN ('Active', 'Overdue', 'NPA')
        """
        
        df_capital = self.db.query_to_dataframe(query_capital)
        df_capital.to_csv(self.export_dir / 'regulatory_capital.csv', index=False)
        logger.info(f"   ✅ Exported capital adequacy")
        
        # 2. Asset Classification
        query_asset = """
        SELECT 
            CASE 
                WHEN days_past_due = 0 THEN 'Standard'
                WHEN days_past_due <= 90 THEN 'Sub-Standard'
                WHEN days_past_due <= 180 THEN 'Doubtful - 1'
                WHEN days_past_due <= 360 THEN 'Doubtful - 2'
                ELSE 'Doubtful - 3'
            END as asset_classification,
            COUNT(*) as loan_count,
            SUM(current_balance) as outstanding_amount,
            SUM(CASE 
                WHEN days_past_due = 0 THEN current_balance * 0.004
                WHEN days_past_due <= 90 THEN current_balance * 0.10
                WHEN days_past_due <= 180 THEN current_balance * 0.25
                WHEN days_past_due <= 360 THEN current_balance * 0.40
                ELSE current_balance * 0.60
            END) as provision_required
        FROM fact_loan
        WHERE loan_status IN ('Active', 'Overdue', 'NPA')
        GROUP BY asset_classification
        """
        
        df_asset = self.db.query_to_dataframe(query_asset)
        df_asset.to_csv(self.export_dir / 'regulatory_asset_classification.csv', index=False)
        logger.info(f"   ✅ Exported asset classification")
        
        return True
    
    def export_all(self):
        logger.info("="*60)
        logger.info("🚀 EXPORTING ALL TABLEAU DASHBOARD DATASETS")
        logger.info("="*60)
        
        try:
            self.export_executive_dashboard()
            self.export_risk_dashboard()
            self.export_fraud_dashboard()
            self.export_collection_dashboard()
            self.export_regulatory_dashboard()
            
            logger.info("="*60)
            logger.info(f"✅ ALL DASHBOARD DATASETS EXPORTED TO: {self.export_dir}")
            logger.info("="*60)
            return True
            
        except Exception as e:
            logger.error(f"❌ Export failed: {e}")
            return False


if __name__ == "__main__":
    exporter = TableauExporter()
    exporter.export_all()