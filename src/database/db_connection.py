import os
import mysql.connector
import pandas as pd
from mysql.connector import Error
from sqlalchemy import create_engine
import logging
from contextlib import contextmanager
from typing import Generator, Dict, Any, Optional
import configparser
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabaseConnection:

    def __init__(self, config_path: str = 'config/database.ini'):
        self.config_path = config_path
        self.config = self._load_config()
        self.engine = None
        self.connection = None
        
    def _load_config(self) -> Dict[str, Any]:
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(
                f"Configuration file not found: {self.config_path}\n"
                f"Please create the file at: {os.path.abspath(self.config_path)}"
            )
        
        config = configparser.ConfigParser()
        config.read(self.config_path)
        
        if 'mysql' not in config:
            raise KeyError(
                "MySQL section not found in config file.\n"
                "Please add [mysql] section to your database.ini"
            )
        
        return {
            'host': config['mysql'].get('host', 'localhost'),
            'database': config['mysql'].get('database', 'creditflow360'),
            'user': config['mysql'].get('user', 'root'),
            'password': config['mysql'].get('password', ''),
            'port': config['mysql'].getint('port', 3306),
            'charset': config['mysql'].get('charset', 'utf8mb4'),
            'use_unicode': config['mysql'].getboolean('use_unicode', True),
            'connect_timeout': config['mysql'].getint('connect_timeout', 10)
        }
    
    @contextmanager
    def get_connection(self):
        conn = None
        try:
            conn = mysql.connector.connect(**self.config)
            yield conn
            conn.commit()
        except Error as e:
            logger.error(f"Database error: {e}")
            if conn:
                conn.rollback()
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()
    def get_sqlalchemy_engine(self):
        if self.engine is None:
            connection_string = (
                f"mysql+mysqlconnector://{self.config['user']}:{self.config['password']}"
                f"@{self.config['host']}:{self.config['port']}/{self.config['database']}"
            )
            self.engine = create_engine(connection_string)
        return self.engine
    
    def test_connection(self) -> Dict[str, Any]:
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT VERSION()")
                version = cursor.fetchone()
                cursor.execute("SELECT DATABASE()")
                database = cursor.fetchone()
                
                return {
                    'status': 'success',
                    'message': 'Connected successfully',
                    'version': version[0] if version else 'Unknown',
                    'database': database[0] if database else self.config['database'],
                    'host': self.config['host'],
                    'user': self.config['user']
                }
        except Exception as e:
            return {
                'status': 'failed',
                'message': str(e),
                'error_type': type(e).__name__
            }
    
    def execute_query(self, query: str, params: tuple = None):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params or ())
            return cursor
        
    def execute_many(self, query: str, params_list: list):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.executemany(query, params_list)
            conn.commit()
            return cursor.rowcount
        

    def query_to_dataframe(self, query: str) -> pd.DataFrame:
        try:
            with self.get_connection() as conn:
                return pd.read_sql(query, conn)
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            logger.error(f"Query: {query[:200]}...")
            raise



    def dataframe_to_table(self, df: pd.DataFrame, table_name: str, 
                          if_exists: str = 'append', chunksize: int = 1000):
        try:
            engine = self.get_sqlalchemy_engine()
            df.to_sql(
                name=table_name,
                con=engine,
                if_exists=if_exists,
                index=False,
                chunksize=chunksize,
                method='multi'
            )
            logger.info(f"✅ Written {len(df)} rows to {table_name}")
        except Exception as e:
            logger.error(f"Error writing to table {table_name}: {e}")
            raise
    
    def database_exists(self) -> bool:
        try:
            config_no_db = self.config.copy()
            config_no_db['database'] = None
            
            conn = mysql.connector.connect(**config_no_db)
            cursor = conn.cursor()
            cursor.execute(f"SHOW DATABASES LIKE '{self.config['database']}'")
            result = cursor.fetchone()
            conn.close()
            return result is not None
        except Exception as e:
            logger.error(f"Error checking database: {e}")
            return False
    
    def create_database(self):
        try:
            config_no_db = self.config.copy()
            config_no_db['database'] = None
            
            conn = mysql.connector.connect(**config_no_db)
            cursor = conn.cursor()
            cursor.execute(
                f"CREATE DATABASE IF NOT EXISTS {self.config['database']} "
                f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            )
            conn.commit()
            conn.close()
            logger.info(f"✅ Database '{self.config['database']}' created/verified")
            return True
        except Exception as e:
            logger.error(f"Error creating database: {e}")
            return False
        
    def use_database(self, database_name):
        self.config['database'] = database_name
        self.engine = None
        logger.info(f"✅ Switched to database: {database_name}")


def test_database_connection():
    print("\n" + "="*50)
    print("🔧 TESTING DATABASE CONNECTION")
    print("="*50)
    
    try:
        db = DatabaseConnection()
        
        result = db.test_connection()
        
        if result['status'] == 'success':
            print(f"✅ SUCCESS! Connected to MySQL")
            print(f"   • Host: {result['host']}")
            print(f"   • Database: {result['database']}")
            print(f"   • User: {result['user']}")
            print(f"   • Version: {result['version']}")
            return True
        else:
            print(f"❌ FAILED: {result['message']}")
            print(f"   Error Type: {result.get('error_type', 'Unknown')}")
            return False
            
    except FileNotFoundError as e:
        print(f"❌ CONFIG ERROR: {e}")
        print("\n💡 TIPS:")
        print("   1. Create config/database.ini file")
        print("   2. Add your MySQL credentials")
        print("   3. Run this script again")
        return False
    except KeyError as e:
        print(f"❌ CONFIG ERROR: {e}")
        return False
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: {e}")
        return False


if __name__ == "__main__":
    test_database_connection()