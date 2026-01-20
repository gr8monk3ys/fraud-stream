"""
Snowflake utilities for data warehouse operations.
Handles connections, table operations, and data loading.
"""
import logging
from typing import Dict, List, Optional, Any, Union
from contextlib import contextmanager
import pandas as pd

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine, text
import snowflake.connector.errors

from src.config import settings

logger = logging.getLogger(__name__)


class SnowflakeManager:
    """Manager for Snowflake data warehouse operations."""
    
    def __init__(self):
        """Initialize Snowflake manager."""
        self.settings = settings
        self._connection = None
        self._engine = None
    
    def get_connection(self) -> snowflake.connector.SnowflakeConnection:
        """Get Snowflake connection."""
        if not self._connection or self._connection.is_closed():
            self._connection = self._create_connection()
        return self._connection
    
    def get_engine(self):
        """Get SQLAlchemy engine for Snowflake."""
        if not self._engine:
            self._engine = self._create_engine()
        return self._engine
    
    def _create_connection(self) -> snowflake.connector.SnowflakeConnection:
        """Create Snowflake connection."""
        config = self.settings.get_snowflake_config()
        
        try:
            connection = snowflake.connector.connect(**config)
            logger.info(f"Connected to Snowflake account: {config['account']}")
            return connection
        except Exception:
            logger.exception("Failed to connect to Snowflake")
            raise
    
    def _create_engine(self):
        """Create SQLAlchemy engine for Snowflake."""
        config = self.settings.get_snowflake_config()
        
        url = URL(
            account=config['account'].replace('.snowflakecomputing.com', ''),
            user=config['user'],
            password=config['password'],
            database=config['database'],
            schema=config['schema'],
            warehouse=config['warehouse'],
            role=config['role'],
        )
        
        engine = create_engine(url)
        logger.info("Created Snowflake SQLAlchemy engine")
        return engine
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> List[Dict]:
        """
        Execute SQL query and return results.
        
        Args:
            query: SQL query to execute
            params: Query parameters
            
        Returns:
            Query results as list of dictionaries
        """
        connection = self.get_connection()
        cursor = connection.cursor(snowflake.connector.DictCursor)
        
        try:
            cursor.execute(query, params)
            results = cursor.fetchall()
            logger.debug(f"Query executed successfully, returned {len(results)} rows")
            return results
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise
        finally:
            cursor.close()
    
    def execute_non_query(self, query: str, params: Optional[Dict] = None) -> int:
        """
        Execute non-query SQL statement (INSERT, UPDATE, DELETE, etc.).
        
        Args:
            query: SQL statement to execute
            params: Query parameters
            
        Returns:
            Number of affected rows
        """
        connection = self.get_connection()
        cursor = connection.cursor()
        
        try:
            cursor.execute(query, params)
            affected_rows = cursor.rowcount
            connection.commit()
            logger.debug(f"Non-query executed successfully, affected {affected_rows} rows")
            return affected_rows
        except Exception as e:
            logger.error(f"Non-query execution failed: {e}")
            connection.rollback()
            raise
        finally:
            cursor.close()
    
    def create_database_structure(self) -> bool:
        """Create the required database structure for the streaming pipeline."""
        try:
            # Create database if not exists
            self.execute_non_query(f"CREATE DATABASE IF NOT EXISTS {self.settings.snowflake.database}")
            
            # Create schemas
            schemas = [
                self.settings.snowflake.bronze_schema,
                self.settings.snowflake.silver_schema,
                self.settings.snowflake.gold_schema
            ]
            
            for schema in schemas:
                self.execute_non_query(
                    f"CREATE SCHEMA IF NOT EXISTS {self.settings.snowflake.database}.{schema}"
                )
            
            # Create warehouse if not exists
            warehouse_sql = f"""
            CREATE WAREHOUSE IF NOT EXISTS {self.settings.snowflake.warehouse}
            WITH 
                WAREHOUSE_SIZE = 'X-SMALL'
                AUTO_SUSPEND = 300
                AUTO_RESUME = TRUE
                MIN_CLUSTER_COUNT = 1
                MAX_CLUSTER_COUNT = 1
                SCALING_POLICY = 'STANDARD'
            """
            self.execute_non_query(warehouse_sql)
            
            logger.info("Database structure created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create database structure: {e}")
            return False
    
    def create_streaming_tables(self) -> bool:
        """Create all required tables for the streaming pipeline."""
        try:
            # Bronze layer - raw transactions
            bronze_transactions_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.settings.snowflake.database}.{self.settings.snowflake.bronze_schema}.TRANSACTIONS_RAW (
                TRANSACTION_ID STRING PRIMARY KEY,
                TIMESTAMP TIMESTAMP_NTZ,
                CUSTOMER_ID STRING,
                CARD_NUMBER_LAST4 STRING,
                CARD_TYPE STRING,
                AMOUNT NUMBER(10,2),
                CURRENCY STRING,
                MERCHANT_ID STRING,
                MERCHANT_NAME STRING,
                MERCHANT_CATEGORY_CODE STRING,
                MERCHANT_CATEGORY STRING,
                LOCATION VARIANT,
                CHANNEL STRING,
                STATUS STRING,
                RISK_SCORE NUMBER(3,2),
                AUTHORIZATION_CODE STRING,
                PROCESSOR_RESPONSE_CODE STRING,
                IS_INTERNATIONAL BOOLEAN,
                CUSTOMER_PRESENT BOOLEAN,
                ENTRY_METHOD STRING,
                METADATA VARIANT,
                CREATED_AT TIMESTAMP_NTZ,
                VERSION STRING,
                -- Kafka metadata
                KAFKA_TOPIC STRING,
                KAFKA_PARTITION INTEGER,
                KAFKA_OFFSET INTEGER,
                KAFKA_TIMESTAMP TIMESTAMP_NTZ,
                INGESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
            CLUSTER BY (TIMESTAMP, CUSTOMER_ID)
            """
            
            self.execute_non_query(bronze_transactions_sql)
            
            # Silver layer - cleaned and validated transactions
            silver_transactions_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.settings.snowflake.database}.{self.settings.snowflake.silver_schema}.TRANSACTIONS_CLEAN (
                TRANSACTION_ID STRING PRIMARY KEY,
                TRANSACTION_DATE DATE,
                TRANSACTION_TIMESTAMP TIMESTAMP_NTZ,
                CUSTOMER_ID_HASH STRING,
                CUSTOMER_ID_ORIGINAL STRING, -- Only for development/testing
                CARD_NUMBER_LAST4 STRING,
                CARD_TYPE STRING,
                AMOUNT NUMBER(10,2),
                CURRENCY STRING,
                MERCHANT_ID STRING,
                MERCHANT_NAME STRING,
                MERCHANT_CATEGORY_CODE STRING,
                MERCHANT_CATEGORY STRING,
                COUNTRY STRING,
                STATE STRING,
                CITY STRING,
                ZIP_CODE STRING,
                LATITUDE NUMBER(10,6),
                LONGITUDE NUMBER(10,6),
                CHANNEL STRING,
                STATUS STRING,
                AUTHORIZATION_CODE STRING,
                PROCESSOR_RESPONSE_CODE STRING,
                IS_INTERNATIONAL BOOLEAN,
                CUSTOMER_PRESENT BOOLEAN,
                ENTRY_METHOD STRING,
                -- Calculated fraud features
                VELOCITY_1H INTEGER,
                VELOCITY_6H INTEGER,
                VELOCITY_24H INTEGER,
                AMOUNT_ZSCORE NUMBER(8,4),
                DISTANCE_FROM_LAST_KM NUMBER(10,2),
                TIME_SINCE_LAST_MINUTES NUMBER(10,2),
                MERCHANT_RISK_SCORE NUMBER(3,2),
                IS_WEEKEND BOOLEAN,
                HOUR_OF_DAY INTEGER,
                DAY_OF_WEEK INTEGER,
                -- Data quality flags
                IS_LATE_ARRIVAL BOOLEAN DEFAULT FALSE,
                IS_DUPLICATE BOOLEAN DEFAULT FALSE,
                DATA_QUALITY_SCORE NUMBER(3,2),
                PROCESSED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                CREATED_AT TIMESTAMP_NTZ
            )
            CLUSTER BY (TRANSACTION_DATE, CUSTOMER_ID_HASH)
            """
            
            self.execute_non_query(silver_transactions_sql)
            
            # Silver layer - fraud alerts
            silver_fraud_alerts_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.settings.snowflake.database}.{self.settings.snowflake.silver_schema}.FRAUD_ALERTS (
                ALERT_ID STRING PRIMARY KEY,
                TRANSACTION_ID STRING,
                CUSTOMER_ID_HASH STRING,
                ALERT_TIMESTAMP TIMESTAMP_NTZ,
                ALERT_TYPE STRING,
                SEVERITY STRING,
                RISK_SCORE NUMBER(3,2),
                CONFIDENCE_SCORE NUMBER(3,2),
                RECOMMENDED_ACTION STRING,
                INVESTIGATOR_ASSIGNED STRING,
                STATUS STRING DEFAULT 'OPEN',
                FEATURES VARIANT,
                TRANSACTION_SUMMARY VARIANT,
                RULES_TRIGGERED ARRAY,
                METADATA VARIANT,
                CREATED_AT TIMESTAMP_NTZ,
                UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
            CLUSTER BY (ALERT_TIMESTAMP, SEVERITY)
            """
            
            self.execute_non_query(silver_fraud_alerts_sql)
            
            # Gold layer - daily transaction aggregates
            gold_daily_transactions_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.settings.snowflake.database}.{self.settings.snowflake.gold_schema}.DAILY_TRANSACTION_SUMMARY (
                TRANSACTION_DATE DATE PRIMARY KEY,
                TOTAL_TRANSACTIONS INTEGER,
                TOTAL_AMOUNT NUMBER(15,2),
                APPROVED_TRANSACTIONS INTEGER,
                APPROVED_AMOUNT NUMBER(15,2),
                DECLINED_TRANSACTIONS INTEGER,
                DECLINED_AMOUNT NUMBER(15,2),
                FRAUD_ALERTS INTEGER,
                HIGH_RISK_TRANSACTIONS INTEGER,
                UNIQUE_CUSTOMERS INTEGER,
                UNIQUE_MERCHANTS INTEGER,
                INTERNATIONAL_TRANSACTIONS INTEGER,
                INTERNATIONAL_AMOUNT NUMBER(15,2),
                AVG_TRANSACTION_AMOUNT NUMBER(10,2),
                MEDIAN_TRANSACTION_AMOUNT NUMBER(10,2),
                MAX_TRANSACTION_AMOUNT NUMBER(10,2),
                CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
            """
            
            self.execute_non_query(gold_daily_transactions_sql)
            
            # Gold layer - customer transaction patterns
            gold_customer_patterns_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.settings.snowflake.database}.{self.settings.snowflake.gold_schema}.CUSTOMER_TRANSACTION_PATTERNS (
                CUSTOMER_ID_HASH STRING PRIMARY KEY,
                ANALYSIS_DATE DATE,
                TOTAL_TRANSACTIONS INTEGER,
                TOTAL_AMOUNT NUMBER(15,2),
                AVG_TRANSACTION_AMOUNT NUMBER(10,2),
                PREFERRED_MERCHANT_CATEGORIES ARRAY,
                PREFERRED_CHANNELS ARRAY,
                TYPICAL_TRANSACTION_HOURS ARRAY,
                HOME_COUNTRY STRING,
                HOME_STATE STRING,
                HOME_CITY STRING,
                INTERNATIONAL_TRANSACTION_PCT NUMBER(5,2),
                WEEKEND_TRANSACTION_PCT NUMBER(5,2),
                RISK_PROFILE STRING,
                FRAUD_SCORE NUMBER(3,2),
                LAST_TRANSACTION_DATE DATE,
                CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
            """
            
            self.execute_non_query(gold_customer_patterns_sql)
            
            # Gold layer - merchant analytics
            gold_merchant_analytics_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.settings.snowflake.database}.{self.settings.snowflake.gold_schema}.MERCHANT_ANALYTICS (
                MERCHANT_ID STRING PRIMARY KEY,
                MERCHANT_NAME STRING,
                MERCHANT_CATEGORY STRING,
                ANALYSIS_DATE DATE,
                TOTAL_TRANSACTIONS INTEGER,
                TOTAL_AMOUNT NUMBER(15,2),
                UNIQUE_CUSTOMERS INTEGER,
                AVG_TRANSACTION_AMOUNT NUMBER(10,2),
                DECLINE_RATE NUMBER(5,2),
                FRAUD_RATE NUMBER(5,2),
                RISK_SCORE NUMBER(3,2),
                COUNTRY STRING,
                STATE STRING,
                CITY STRING,
                CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
            """
            
            self.execute_non_query(gold_merchant_analytics_sql)
            
            logger.info("All streaming tables created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create streaming tables: {e}")
            return False
    
    def bulk_insert_dataframe(
        self, 
        df: pd.DataFrame, 
        table_name: str, 
        schema: str,
        chunk_size: int = 10000,
        if_exists: str = 'append'
    ) -> bool:
        """
        Bulk insert DataFrame into Snowflake table.
        
        Args:
            df: DataFrame to insert
            table_name: Target table name
            schema: Target schema
            chunk_size: Number of rows per chunk
            if_exists: What to do if table exists ('append', 'replace', 'fail')
            
        Returns:
            Success status
        """
        try:
            connection = self.get_connection()
            
            # Write DataFrame to Snowflake
            success, num_chunks, num_rows, output = write_pandas(
                conn=connection,
                df=df,
                table_name=table_name,
                schema=schema,
                database=self.settings.snowflake.database,
                chunk_size=chunk_size,
                compression='gzip',
                on_error='continue',
                parallel=4,
                quote_identifiers=False
            )
            
            if success:
                logger.info(f"Successfully inserted {num_rows} rows into {schema}.{table_name}")
                return True
            else:
                logger.error(f"Failed to insert data into {schema}.{table_name}")
                return False
                
        except Exception as e:
            logger.error(f"Bulk insert failed: {e}")
            return False
    
    def upsert_data(
        self, 
        source_table: str, 
        target_table: str, 
        merge_keys: List[str],
        update_columns: Optional[List[str]] = None
    ) -> int:
        """
        Perform MERGE (upsert) operation.
        
        Args:
            source_table: Source table name
            target_table: Target table name  
            merge_keys: Columns to match on
            update_columns: Columns to update (if None, update all)
            
        Returns:
            Number of rows affected
        """
        try:
            # Build MERGE statement
            merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])
            
            if update_columns:
                update_set = ", ".join([f"{col} = source.{col}" for col in update_columns])
            else:
                # Get all columns except merge keys for update
                columns_query = f"""
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = '{target_table.split('.')[-1].upper()}'
                AND COLUMN_NAME NOT IN ({','.join([f"'{k.upper()}'" for k in merge_keys])})
                """
                columns = self.execute_query(columns_query)
                update_set = ", ".join([f"{col['COLUMN_NAME']} = source.{col['COLUMN_NAME']}" for col in columns])
            
            merge_sql = f"""
            MERGE INTO {target_table} AS target
            USING {source_table} AS source
            ON {merge_condition}
            WHEN MATCHED THEN
                UPDATE SET {update_set}, UPDATED_AT = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN
                INSERT SELECT source.*
            """
            
            affected_rows = self.execute_non_query(merge_sql)
            logger.info(f"MERGE operation completed, {affected_rows} rows affected")
            return affected_rows
            
        except Exception as e:
            logger.error(f"MERGE operation failed: {e}")
            raise
    
    def get_table_stats(self, table_name: str, schema: str) -> Dict[str, Any]:
        """Get table statistics."""
        try:
            stats_query = f"""
            SELECT 
                COUNT(*) as row_count,
                COUNT(DISTINCT CUSTOMER_ID) as unique_customers,
                MIN(TIMESTAMP) as earliest_transaction,
                MAX(TIMESTAMP) as latest_transaction,
                SUM(AMOUNT) as total_amount
            FROM {self.settings.snowflake.database}.{schema}.{table_name}
            """
            
            result = self.execute_query(stats_query)
            return result[0] if result else {}
            
        except Exception as e:
            logger.error(f"Failed to get table stats: {e}")
            return {}
    
    def close_connections(self):
        """Close all connections."""
        if self._connection and not self._connection.is_closed():
            self._connection.close()
            self._connection = None
        
        if self._engine:
            self._engine.dispose()
            self._engine = None
        
        logger.info("Snowflake connections closed")


@contextmanager
def snowflake_connection():
    """Context manager for Snowflake operations."""
    manager = SnowflakeManager()
    try:
        yield manager
    finally:
        manager.close_connections()
