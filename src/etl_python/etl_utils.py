import pandas as pd
import numpy as np
from datetime import datetime
import hashlib
import logging
from typing import Dict, List, Tuple, Any, Optional
import yaml
import os
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ETLUtils:    
    @staticmethod
    def load_config(config_path: str = 'config/etl_config.yaml') -> dict:
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            logger.info(f"✅ Configuration loaded from {config_path}")
            return config
        except FileNotFoundError:
            logger.warning(f"Configuration file not found: {config_path}. Using defaults.")
            return {
                'etl': {
                    'batch_size': 10000,
                    'data_paths': {'raw': 'data/raw_csv/'},
                    'source_files': {},
                    'target_tables': {}
                }
            }
    
    @staticmethod
    def generate_surrogate_key(prefix: str, *args) -> str:
        combined = ''.join(str(arg) for arg in args)
        hash_obj = hashlib.md5(combined.encode())
        return f"{prefix}{hash_obj.hexdigest()[:8].upper()}"
    
    @staticmethod
    def date_to_sk(date_value) -> Optional[int]:
        if pd.isna(date_value) or date_value is None:
            return None
        
        try:
            if isinstance(date_value, str):
                date_obj = pd.to_datetime(date_value)
            else:
                date_obj = date_value
            
            return int(date_obj.strftime('%Y%m%d'))
        except:
            return None
    
    @staticmethod
    def validate_dataframe(df: pd.DataFrame, required_columns: List[str]) -> bool:
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            logger.error(f"Missing required columns: {missing_cols}")
            return False
        return True
    
    @staticmethod
    def clean_numeric(value) -> Optional[float]:
        if pd.isna(value) or value is None:
            return None
        try:
            return float(value)
        except:
            return None
    
    @staticmethod
    def clean_string(value) -> Optional[str]:
        if pd.isna(value) or value is None:
            return None
        return str(value).strip()
    
    @staticmethod
    def get_batch_ranges(total_records: int, batch_size: int) -> List[Tuple[int, int]]:
        batches = []
        for i in range(0, total_records, batch_size):
            end = min(i + batch_size, total_records)
            batches.append((i, end))
        return batches
    
    @staticmethod
    def log_etl_step(step_name: str, status: str, records: int = 0, error: str = None):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_msg = f"[{timestamp}] {step_name}: {status}"
        if records:
            log_msg += f" - Records: {records}"
        if error:
            log_msg += f" - Error: {error}"
        
        logger.info(log_msg)

    @staticmethod
    def create_etl_control_record(db_connection, etl_name: str, table_name: str, 
                                 status: str, records: int, error: str = None):
        query = """
        INSERT INTO etl_control 
        (etl_name, table_name, last_run, status, records_processed, error_message)
        VALUES (%s, %s, NOW(), %s, %s, %s)
        """
        if hasattr(records, 'item'):
            records = records.item()
        
        params = (etl_name, table_name, status, int(records), error)
        
        try:
            db_connection.execute_query(query, params)
            logger.info(f"✅ ETL control record inserted for {table_name}")
        except Exception as e:
            logger.error(f"❌ Failed to insert ETL control record: {e}")
    


class DataQualityChecker:
    @staticmethod
    def check_completeness(df: pd.DataFrame, critical_columns: List[str]) -> Dict:
        results = {}
        for col in critical_columns:
            if col in df.columns:
                null_count = df[col].isnull().sum()
                null_percentage = (null_count / len(df)) * 100 if len(df) > 0 else 0
                results[col] = {
                    'null_count': null_count,
                    'null_percentage': round(null_percentage, 2),
                    'passed': null_percentage < 5  # Less than 5% nulls
                }
        return results
    
    @staticmethod
    def check_uniqueness(df: pd.DataFrame, unique_columns: List[str]) -> Dict:
        results = {}
        for col in unique_columns:
            if col in df.columns:
                duplicate_count = df[col].duplicated().sum()
                duplicate_percentage = (duplicate_count / len(df)) * 100 if len(df) > 0 else 0
                results[col] = {
                    'duplicate_count': duplicate_count,
                    'duplicate_percentage': round(duplicate_percentage, 2),
                    'passed': duplicate_count == 0
                }
        return results
    
    @staticmethod
    def check_range(df: pd.DataFrame, range_checks: Dict) -> Dict:
        results = {}
        for col, ranges in range_checks.items():
            if col in df.columns:
                out_of_range = df[
                    (df[col] < ranges['min']) | 
                    (df[col] > ranges['max'])
                ].shape[0]
                results[col] = {
                    'out_of_range': out_of_range,
                    'min_value': df[col].min() if not df[col].isnull().all() else None,
                    'max_value': df[col].max() if not df[col].isnull().all() else None,
                    'passed': out_of_range == 0
                }
        return results
    
    @staticmethod
    def generate_quality_report(df: pd.DataFrame, table_name: str) -> Dict:
        if df.empty:
            return {
                'table_name': table_name,
                'total_records': 0,
                'quality_score': 100,
                'column_profiles': {}
            }
        
        report = {
            'table_name': table_name,
            'total_records': len(df),
            'total_columns': len(df.columns),
            'memory_usage': f"{df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB",
            'column_profiles': {},
            'quality_score': 0
        }
        
        for col in df.columns:
            col_profile = {
                'dtype': str(df[col].dtype),
                'null_count': int(df[col].isnull().sum()),
                'null_percentage': round((df[col].isnull().sum() / len(df)) * 100, 2) if len(df) > 0 else 0,
                'unique_count': int(df[col].nunique()),
                'unique_percentage': round((df[col].nunique() / len(df)) * 100, 2) if len(df) > 0 else 0
            }
            
            if pd.api.types.is_numeric_dtype(df[col]):
                col_profile['min'] = float(df[col].min()) if not df[col].isnull().all() else None
                col_profile['max'] = float(df[col].max()) if not df[col].isnull().all() else None
                col_profile['mean'] = float(df[col].mean()) if not df[col].isnull().all() else None
                col_profile['std'] = float(df[col].std()) if not df[col].isnull().all() else None
            
            report['column_profiles'][col] = col_profile
        
        null_penalty = sum(p['null_percentage'] for p in report['column_profiles'].values()) / len(df.columns) if len(df.columns) > 0 else 0
        report['quality_score'] = round(100 - null_penalty, 2)
        
        return report


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, pd.Timestamp)):
            return obj.isoformat()
        if isinstance(obj, pd.Series):
            return obj.tolist()
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super().default(obj)