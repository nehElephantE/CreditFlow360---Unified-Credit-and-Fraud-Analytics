import pandas as pd
from datetime import datetime, timedelta
import logging
from typing import Optional
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from src.database.db_connection import DatabaseConnection
from src.etl_python.etl_utils import ETLUtils, DataQualityChecker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DateDimensionLoader:
    def __init__(self, db_connection: DatabaseConnection):
        self.db = db_connection
        self.utils = ETLUtils()
        self.quality_checker = DataQualityChecker()
    
    def generate_date_range(self, start_date: str = '2022-01-01', 
                          end_date: str = '2026-12-31') -> pd.DataFrame:
        logger.info(f"📅 Generating date dimension from {start_date} to {end_date}")
        
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        
        date_range = pd.date_range(start=start, end=end, freq='D')
        
        dates = []
        for date in date_range:
            year = date.year
            month = date.month
            day = date.day
            
            # Financial year in India (April to March)
            if month >= 4:
                fin_year = f"FY{year}-{year+1}"
            else:
                fin_year = f"FY{year-1}-{year}"
            
            date_sk = int(date.strftime('%Y%m%d'))
            
            dates.append({
                'date_sk': date_sk,
                'full_date': date.date(),
                'day': day,
                'month': month,
                'month_name': date.strftime('%B'),
                'quarter': (month - 1) // 3 + 1,
                'year': year,
                'week': date.isocalendar()[1],
                'weekday': date.strftime('%A'),
                'is_weekend': 1 if date.weekday() >= 5 else 0,
                'is_holiday': 0,  # Can be populated later
                'financial_year': fin_year
            })
        
        df = pd.DataFrame(dates)
        logger.info(f"✅ Generated {len(df)} date records")
        return df
    
    def load_date_dimension(self, start_date: str = '2022-01-01', 
                           end_date: str = '2026-12-31') -> int:
        logger.info("=" * 60)
        logger.info("🚀 LOADING DATE DIMENSION")
        logger.info("=" * 60)
        
        try:
            df_dates = self.generate_date_range(start_date, end_date)
            
            quality_report = self.quality_checker.generate_quality_report(
                df_dates, 'dim_date'
            )
            logger.info(f"📊 Data Quality Score: {quality_report['quality_score']}%")
            
            check_query = "SELECT COUNT(*) as count FROM dim_date"
            result = self.db.query_to_dataframe(check_query)
            existing_count = result['count'].iloc[0] if not result.empty else 0
            
            if existing_count > 0:
                logger.info(f"📅 Date dimension already has {existing_count} records, skipping...")
                return existing_count
            
            
            batch_size = 1000
            batches = self.utils.get_batch_ranges(len(df_dates), batch_size)
            
            total_loaded = 0
            for i, (start_idx, end_idx) in enumerate(batches):
                batch_df = df_dates.iloc[start_idx:end_idx]
                
                for _, row in batch_df.iterrows():
                    query = """
                    INSERT INTO dim_date 
                    (date_sk, full_date, day, month, month_name, quarter, year, 
                     week, weekday, is_weekend, is_holiday, financial_year)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        day = VALUES(day),
                        month = VALUES(month),
                        month_name = VALUES(month_name),
                        quarter = VALUES(quarter),
                        year = VALUES(year),
                        week = VALUES(week),
                        weekday = VALUES(weekday),
                        is_weekend = VALUES(is_weekend),
                        financial_year = VALUES(financial_year)
                    """
                    self.db.execute_query(query, tuple(row))
                
                total_loaded += len(batch_df)
                logger.info(f"  📦 Batch {i+1}/{len(batches)}: Loaded {len(batch_df)} records")
            
            result = self.db.query_to_dataframe("SELECT COUNT(*) as count FROM dim_date")
            count = result['count'].iloc[0]
            
            logger.info(f"✅ Date dimension loaded successfully: {count} records")
            
            self.utils.create_etl_control_record(
                self.db, 
                "DATE_DIMENSION_LOAD", 
                "dim_date", 
                "SUCCESS", 
                count
            )
            
            return count
            
        except Exception as e:
            logger.error(f"❌ Failed to load date dimension: {e}")
            self.utils.create_etl_control_record(
                self.db, 
                "DATE_DIMENSION_LOAD", 
                "dim_date", 
                "FAILED", 
                0, 
                str(e)
            )
            raise


if __name__ == "__main__":
    db = DatabaseConnection()
    loader = DateDimensionLoader(db)
    count = loader.load_date_dimension()
    print(f"✅ Loaded {count} date records")