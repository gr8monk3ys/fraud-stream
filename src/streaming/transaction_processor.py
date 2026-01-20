"""
Main Spark Structured Streaming processor for financial transactions.
Handles real-time ingestion, transformation, and loading to Snowflake.
"""
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import json

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, when, current_timestamp, current_date, unix_timestamp,
    from_json, to_json, window, count, sum as spark_sum, avg, max as spark_max,
    lag, lead, row_number, dense_rank, regexp_extract, split, coalesce,
    hash, sha2, date_format, hour, dayofweek, isnan, isnull,
    from_unixtime, to_timestamp, expr, struct, array, map_from_arrays
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, 
    BooleanType, TimestampType, DateType, DecimalType, ArrayType, MapType
)
from pyspark.sql.window import Window
from pyspark.sql.streaming import StreamingQuery

from src.config import settings
from src.utils.security import SecurityManager, mask_transaction_for_logging
from src.utils.data_quality import DataQualityValidator
from src.utils.snowflake_utils import SnowflakeManager

logger = logging.getLogger(__name__)


class TransactionProcessor:
    """Main processor for streaming financial transactions."""
    
    def __init__(self):
        """Initialize transaction processor."""
        self.settings = settings
        self.spark = self._create_spark_session()
        self.security_manager = SecurityManager()
        self.dq_validator = DataQualityValidator()
        self.snowflake_manager = SnowflakeManager()
        
        # Define schemas
        self.transaction_schema = self._get_transaction_schema()
        self.checkpoint_location = self.settings.spark.checkpoint_location
        
        self.running_queries = []
    
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with required configurations."""
        builder = SparkSession.builder \
            .appName(self.settings.spark.app_name) \
            .config("spark.sql.streaming.checkpointLocation", self.checkpoint_location) \
            .config("spark.sql.streaming.stateStore.maintenanceInterval", "60s") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        
        # Add Kafka and Snowflake configurations
        builder = builder \
            .config("spark.jars.packages", 
                   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                   "net.snowflake:snowflake-jdbc:3.14.4,"
                   "net.snowflake:spark-snowflake_2.12:2.11.3-spark_3.5") \
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        
        # Memory and performance settings
        if not self.settings.is_production:
            builder = builder \
                .config("spark.driver.memory", self.settings.spark.driver_memory) \
                .config("spark.executor.memory", self.settings.spark.executor_memory) \
                .config("spark.sql.streaming.metricsEnabled", "true")
        
        return builder.getOrCreate()
    
    def _get_transaction_schema(self) -> StructType:
        """Define the schema for transaction data."""
        return StructType([
            StructField("transaction_id", StringType(), False),
            StructField("timestamp", TimestampType(), False),  # Will convert from long
            StructField("customer_id", StringType(), False),
            StructField("card_number_last4", StringType(), True),
            StructField("card_type", StringType(), True),
            StructField("amount", DecimalType(10, 2), False),
            StructField("currency", StringType(), False),
            StructField("merchant_id", StringType(), False),
            StructField("merchant_name", StringType(), True),
            StructField("merchant_category_code", StringType(), True),
            StructField("merchant_category", StringType(), True),
            StructField("location", StructType([
                StructField("country", StringType(), True),
                StructField("state", StringType(), True),
                StructField("city", StringType(), True),
                StructField("zip_code", StringType(), True),
                StructField("latitude", DoubleType(), True),
                StructField("longitude", DoubleType(), True)
            ]), True),
            StructField("channel", StringType(), True),
            StructField("status", StringType(), True),
            StructField("risk_score", DoubleType(), True),
            StructField("authorization_code", StringType(), True),
            StructField("processor_response_code", StringType(), True),
            StructField("is_international", BooleanType(), True),
            StructField("customer_present", BooleanType(), True),
            StructField("entry_method", StringType(), True),
            StructField("metadata", MapType(StringType(), StringType()), True),
            StructField("created_at", TimestampType(), True),
            StructField("version", StringType(), True)
        ])
    
    def read_from_kafka(self) -> DataFrame:
        """Read streaming data from Kafka."""
        logger.info(f"Reading from Kafka topic: {self.settings.kafka.transactions_topic}")
        
        kafka_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.settings.kafka.bootstrap_servers) \
            .option("subscribe", self.settings.kafka.transactions_topic) \
            .option("startingOffsets", "latest") \
            .option("maxOffsetsPerTrigger", self.settings.spark.max_offsets_per_trigger) \
            .option("kafka.security.protocol", self.settings.kafka.security_protocol) \
            .load()
        
        # Add Kafka metadata columns
        kafka_with_metadata = kafka_df.select(
            col("value").cast("string").alias("json_data"),
            col("key").cast("string").alias("kafka_key"),
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp")
        )
        
        return kafka_with_metadata
    
    def parse_transactions(self, kafka_df: DataFrame) -> DataFrame:
        """Parse JSON transactions from Kafka."""
        logger.info("Parsing transaction data from Kafka messages")
        
        # Parse JSON data
        parsed_df = kafka_df.select(
            from_json(col("json_data"), self.transaction_schema).alias("transaction"),
            col("kafka_key"),
            col("kafka_topic"),
            col("kafka_partition"), 
            col("kafka_offset"),
            col("kafka_timestamp")
        )
        
        # Flatten the transaction structure and add Kafka metadata
        flattened_df = parsed_df.select(
            col("transaction.*"),
            col("kafka_topic"),
            col("kafka_partition"),
            col("kafka_offset"),
            col("kafka_timestamp"),
            current_timestamp().alias("ingested_at")
        )
        
        # Convert timestamp from milliseconds to timestamp if needed
        processed_df = flattened_df.withColumn(
            "timestamp",
            when(col("timestamp").isNull(), 
                 from_unixtime(col("created_at") / 1000))
            .otherwise(col("timestamp"))
        )
        
        return processed_df
    
    def apply_data_quality_checks(self, df: DataFrame) -> DataFrame:
        """Apply data quality validations and flag issues."""
        logger.info("Applying data quality checks")
        
        # Check for required fields
        quality_df = df.withColumn(
            "dq_missing_required",
            when(col("transaction_id").isNull() | 
                 col("customer_id").isNull() |
                 col("amount").isNull() |
                 col("merchant_id").isNull(), True).otherwise(False)
        )
        
        # Check for valid amounts
        quality_df = quality_df.withColumn(
            "dq_invalid_amount",
            when((col("amount") <= 0) | (col("amount") > 50000), True).otherwise(False)
        )
        
        # Check for future timestamps (more than 1 hour in future)
        quality_df = quality_df.withColumn(
            "dq_future_timestamp",
            when(col("timestamp") > expr("current_timestamp() + interval 1 hour"), True)
            .otherwise(False)
        )
        
        # Check for valid status
        valid_statuses = ["PENDING", "APPROVED", "DECLINED", "CANCELLED", "REFUNDED"]
        quality_df = quality_df.withColumn(
            "dq_invalid_status",
            when(~col("status").isin(valid_statuses), True).otherwise(False)
        )
        
        # Overall data quality score (0-1)
        quality_df = quality_df.withColumn(
            "data_quality_score",
            when(col("dq_missing_required") | col("dq_invalid_amount") | 
                 col("dq_future_timestamp") | col("dq_invalid_status"), 0.0)
            .otherwise(1.0)
        )
        
        # Flag late arrivals (more than 2 hours old)
        quality_df = quality_df.withColumn(
            "is_late_arrival",
            when(col("timestamp") < expr("current_timestamp() - interval 2 hour"), True)
            .otherwise(False)
        )
        
        return quality_df
    
    def apply_security_transformations(self, df: DataFrame) -> DataFrame:
        """Apply PII masking and security transformations."""
        logger.info("Applying security transformations")
        
        if not self.settings.security.pii_masking_enabled:
            return df.withColumn("customer_id_hash", col("customer_id")) \
                    .withColumn("customer_id_original", col("customer_id"))
        
        # Hash customer ID using SHA256 with salt
        salt = self.settings.security.customer_id_salt if hasattr(self.settings.security, 'customer_id_salt') else "default_salt"
        
        secured_df = df.withColumn(
            "customer_id_hash",
            sha2(expr(f"concat('{salt}', customer_id)"), 256)
        )
        
        # Keep original customer ID for development/testing
        if not self.settings.is_production:
            secured_df = secured_df.withColumn("customer_id_original", col("customer_id"))
        
        # Remove original customer_id in production
        if self.settings.is_production:
            secured_df = secured_df.drop("customer_id")
        
        return secured_df
    
    def calculate_fraud_features(self, df: DataFrame) -> DataFrame:
        """Calculate fraud detection features."""
        logger.info("Calculating fraud detection features")
        
        # Window specifications for velocity calculations
        customer_window_1h = Window.partitionBy("customer_id_hash") \
            .orderBy(col("timestamp").cast("long")) \
            .rangeBetween(-3600, 0)  # 1 hour in seconds
        
        customer_window_6h = Window.partitionBy("customer_id_hash") \
            .orderBy(col("timestamp").cast("long")) \
            .rangeBetween(-21600, 0)  # 6 hours in seconds
        
        customer_window_24h = Window.partitionBy("customer_id_hash") \
            .orderBy(col("timestamp").cast("long")) \
            .rangeBetween(-86400, 0)  # 24 hours in seconds
        
        # Calculate velocity features
        fraud_df = df.withColumn(
            "velocity_1h",
            count("*").over(customer_window_1h) - 1  # Exclude current transaction
        ).withColumn(
            "velocity_6h", 
            count("*").over(customer_window_6h) - 1
        ).withColumn(
            "velocity_24h",
            count("*").over(customer_window_24h) - 1
        )
        
        # Calculate amount statistics for anomaly detection
        customer_amount_window = Window.partitionBy("customer_id_hash") \
            .orderBy(col("timestamp").cast("long")) \
            .rowsBetween(-50, -1)  # Last 50 transactions
        
        fraud_df = fraud_df.withColumn(
            "customer_avg_amount",
            avg("amount").over(customer_amount_window)
        ).withColumn(
            "customer_stddev_amount",
            expr("stddev(amount)").over(customer_amount_window)
        )
        
        # Calculate Z-score for amount anomaly detection
        fraud_df = fraud_df.withColumn(
            "amount_zscore",
            when(col("customer_stddev_amount") > 0,
                 (col("amount") - col("customer_avg_amount")) / col("customer_stddev_amount"))
            .otherwise(0.0)
        )
        
        # Calculate distance from last transaction
        customer_location_window = Window.partitionBy("customer_id_hash") \
            .orderBy("timestamp") \
            .rowsBetween(-1, -1)  # Previous row
        
        fraud_df = fraud_df.withColumn(
            "prev_latitude",
            lag("location.latitude").over(customer_location_window)
        ).withColumn(
            "prev_longitude", 
            lag("location.longitude").over(customer_location_window)
        )
        
        # Calculate distance using Haversine formula (simplified)
        fraud_df = fraud_df.withColumn(
            "distance_from_last_km",
            when(col("prev_latitude").isNotNull() & col("prev_longitude").isNotNull() &
                 col("location.latitude").isNotNull() & col("location.longitude").isNotNull(),
                 expr("""
                 6371 * acos(
                     cos(radians(location.latitude)) * cos(radians(prev_latitude)) *
                     cos(radians(prev_longitude) - radians(location.longitude)) +
                     sin(radians(location.latitude)) * sin(radians(prev_latitude))
                 )
                 """))
            .otherwise(0.0)
        )
        
        # Time-based features
        fraud_df = fraud_df.withColumn("hour_of_day", hour("timestamp")) \
                          .withColumn("day_of_week", dayofweek("timestamp")) \
                          .withColumn("is_weekend", 
                                    when(dayofweek("timestamp").isin([1, 7]), True).otherwise(False))
        
        # Calculate time since last transaction
        fraud_df = fraud_df.withColumn(
            "prev_timestamp",
            lag("timestamp").over(customer_location_window)
        ).withColumn(
            "time_since_last_minutes",
            when(col("prev_timestamp").isNotNull(),
                 (unix_timestamp("timestamp") - unix_timestamp("prev_timestamp")) / 60.0)
            .otherwise(0.0)
        )
        
        # Merchant risk score (from metadata or default calculation)
        fraud_df = fraud_df.withColumn(
            "merchant_risk_score",
            coalesce(
                col("metadata").getItem("merchant_risk_score").cast(DoubleType()),
                when(col("merchant_category") == "ONLINE", 0.4)
                .when(col("merchant_category") == "OTHER", 0.35)
                .when(col("merchant_category") == "ENTERTAINMENT", 0.3)
                .otherwise(0.2)
            )
        )
        
        return fraud_df.drop("prev_latitude", "prev_longitude", "prev_timestamp",
                           "customer_avg_amount", "customer_stddev_amount")
    
    def write_to_bronze(self, df: DataFrame) -> StreamingQuery:
        """Write raw data to bronze layer in Snowflake."""
        logger.info("Writing to Snowflake bronze layer")
        
        # Prepare data for bronze (raw) layer
        bronze_df = df.select(
            col("transaction_id"),
            col("timestamp"),
            col("customer_id").alias("customer_id_original") if "customer_id" in df.columns else lit(None).alias("customer_id_original"),
            col("customer_id_hash").alias("customer_id"),  # Use hashed version as primary
            col("card_number_last4"),
            col("card_type"),
            col("amount"),
            col("currency"),
            col("merchant_id"),
            col("merchant_name"),
            col("merchant_category_code"),
            col("merchant_category"),
            to_json(col("location")).alias("location"),  # Store as JSON string
            col("channel"),
            col("status"),
            col("risk_score"),
            col("authorization_code"),
            col("processor_response_code"),
            col("is_international"),
            col("customer_present"),
            col("entry_method"),
            to_json(col("metadata")).alias("metadata"),  # Store as JSON string
            col("created_at"),
            col("version"),
            col("kafka_topic"),
            col("kafka_partition"),
            col("kafka_offset"),
            col("kafka_timestamp"),
            col("ingested_at")
        )
        
        # Write to Snowflake
        query = bronze_df.writeStream \
            .outputMode("append") \
            .format("net.snowflake.spark.snowflake") \
            .option("sfURL", self.settings.snowflake.account) \
            .option("sfUser", self.settings.snowflake.user) \
            .option("sfPassword", self.settings.snowflake.password) \
            .option("sfDatabase", self.settings.snowflake.database) \
            .option("sfSchema", self.settings.snowflake.bronze_schema) \
            .option("sfWarehouse", self.settings.snowflake.warehouse) \
            .option("dbtable", "TRANSACTIONS_RAW") \
            .option("checkpointLocation", f"{self.checkpoint_location}/bronze") \
            .trigger(processingTime=self.settings.spark.trigger_interval) \
            .start()
        
        return query
    
    def write_to_silver(self, df: DataFrame) -> StreamingQuery:
        """Write cleaned and enriched data to silver layer."""
        logger.info("Writing to Snowflake silver layer")

        # Filter by configurable quality threshold
        quality_threshold = self.settings.data_quality.silver_quality_threshold
        silver_df = df.filter(col("data_quality_score") > quality_threshold)
        
        silver_df = silver_df.select(
            col("transaction_id"),
            to_date(col("timestamp")).alias("transaction_date"),
            col("timestamp").alias("transaction_timestamp"),
            col("customer_id_hash"),
            col("customer_id_original") if "customer_id_original" in df.columns else lit(None).alias("customer_id_original"),
            col("card_number_last4"),
            col("card_type"),
            col("amount"),
            col("currency"),
            col("merchant_id"),
            col("merchant_name"),
            col("merchant_category_code"),
            col("merchant_category"),
            col("location.country").alias("country"),
            col("location.state").alias("state"),
            col("location.city").alias("city"),
            col("location.zip_code").alias("zip_code"),
            col("location.latitude").alias("latitude"),
            col("location.longitude").alias("longitude"),
            col("channel"),
            col("status"),
            col("authorization_code"),
            col("processor_response_code"),
            col("is_international"),
            col("customer_present"),
            col("entry_method"),
            # Fraud features
            col("velocity_1h"),
            col("velocity_6h"),
            col("velocity_24h"),
            col("amount_zscore"),
            col("distance_from_last_km"),
            col("time_since_last_minutes"),
            col("merchant_risk_score"),
            col("is_weekend"),
            col("hour_of_day"),
            col("day_of_week"),
            # Data quality flags
            col("is_late_arrival"),
            lit(False).alias("is_duplicate"),  # Will be handled by Snowflake MERGE
            col("data_quality_score"),
            current_timestamp().alias("processed_at"),
            col("created_at")
        )
        
        # Write to Snowflake silver layer
        query = silver_df.writeStream \
            .outputMode("append") \
            .format("net.snowflake.spark.snowflake") \
            .option("sfURL", self.settings.snowflake.account) \
            .option("sfUser", self.settings.snowflake.user) \
            .option("sfPassword", self.settings.snowflake.password) \
            .option("sfDatabase", self.settings.snowflake.database) \
            .option("sfSchema", self.settings.snowflake.silver_schema) \
            .option("sfWarehouse", self.settings.snowflake.warehouse) \
            .option("dbtable", "TRANSACTIONS_CLEAN") \
            .option("checkpointLocation", f"{self.checkpoint_location}/silver") \
            .trigger(processingTime=self.settings.spark.trigger_interval) \
            .start()
        
        return query
    
    def run_streaming_pipeline(self) -> List[StreamingQuery]:
        """Run the complete streaming pipeline."""
        logger.info("Starting financial transaction streaming pipeline")
        
        try:
            # Read from Kafka
            kafka_df = self.read_from_kafka()
            
            # Parse transactions
            parsed_df = self.parse_transactions(kafka_df)
            
            # Apply watermark for late data handling
            watermarked_df = parsed_df.withWatermark("timestamp", self.settings.spark.watermark_delay)
            
            # Apply data quality checks
            quality_df = self.apply_data_quality_checks(watermarked_df)
            
            # Apply security transformations
            secured_df = self.apply_security_transformations(quality_df)
            
            # Calculate fraud features
            features_df = self.calculate_fraud_features(secured_df)
            
            # Start streaming queries
            bronze_query = self.write_to_bronze(features_df)
            silver_query = self.write_to_silver(features_df)
            
            queries = [bronze_query, silver_query]
            self.running_queries.extend(queries)
            
            logger.info(f"Started {len(queries)} streaming queries")
            return queries
            
        except Exception:
            logger.exception("Failed to start streaming pipeline")
            raise
    
    def stop_streaming_pipeline(self):
        """Stop all streaming queries gracefully."""
        logger.info("Stopping streaming pipeline")
        
        for query in self.running_queries:
            try:
                query.stop()
                logger.info(f"Stopped query: {query.name}")
            except Exception as e:
                logger.error(f"Error stopping query {query.name}: {e}")
        
        self.running_queries.clear()
        
        # Stop Spark session
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")
    
    def get_streaming_status(self) -> Dict[str, Any]:
        """Get status of all running streaming queries."""
        status = {
            "pipeline_status": "RUNNING" if self.running_queries else "STOPPED",
            "active_queries": len(self.running_queries),
            "queries": []
        }
        
        for query in self.running_queries:
            try:
                query_status = {
                    "id": query.id,
                    "name": query.name,
                    "status": "ACTIVE" if query.isActive else "TERMINATED",
                    "last_progress": query.lastProgress
                }
                status["queries"].append(query_status)
            except Exception as e:
                logger.error(f"Error getting query status: {e}")
        
        return status


def main():
    """Main function to run the transaction processor."""
    processor = TransactionProcessor()
    
    try:
        # Start the streaming pipeline
        queries = processor.run_streaming_pipeline()
        
        # Wait for termination
        for query in queries:
            query.awaitTermination()
            
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, stopping pipeline...")
    except Exception:
        logger.exception("Pipeline error")
        raise
    finally:
        processor.stop_streaming_pipeline()


if __name__ == "__main__":
    main()
