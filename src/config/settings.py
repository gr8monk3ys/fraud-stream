"""
Configuration settings for the Financial Transaction Streaming Pipeline.
Handles environment variables and application configuration.
"""
import os
from typing import List, Optional, Dict, Any
from pydantic import BaseSettings, Field, validator
from pydantic_settings import SettingsConfigDict


class KafkaSettings(BaseSettings):
    """Kafka configuration settings."""
    
    bootstrap_servers: str = Field(default="localhost:9092", env="KAFKA_BOOTSTRAP_SERVERS")
    security_protocol: str = Field(default="PLAINTEXT", env="KAFKA_SECURITY_PROTOCOL")
    sasl_mechanism: str = Field(default="PLAIN", env="KAFKA_SASL_MECHANISM")
    sasl_username: Optional[str] = Field(default=None, env="KAFKA_SASL_USERNAME")
    sasl_password: Optional[str] = Field(default=None, env="KAFKA_SASL_PASSWORD")
    
    # Schema Registry
    schema_registry_url: str = Field(default="http://localhost:8081", env="SCHEMA_REGISTRY_URL")
    schema_registry_username: Optional[str] = Field(default=None, env="SCHEMA_REGISTRY_USERNAME")
    schema_registry_password: Optional[str] = Field(default=None, env="SCHEMA_REGISTRY_PASSWORD")
    
    # Topics
    transactions_topic: str = Field(default="financial.transactions.raw", env="TRANSACTIONS_TOPIC")
    fraud_alerts_topic: str = Field(default="financial.fraud.alerts", env="FRAUD_ALERTS_TOPIC")
    dlq_topic: str = Field(default="financial.transactions.dlq", env="DLQ_TOPIC")
    
    # Consumer settings
    auto_offset_reset: str = Field(default="earliest", env="KAFKA_AUTO_OFFSET_RESET")
    enable_auto_commit: bool = Field(default=True, env="KAFKA_ENABLE_AUTO_COMMIT")
    session_timeout_ms: int = Field(default=30000, env="KAFKA_SESSION_TIMEOUT_MS")


class SnowflakeSettings(BaseSettings):
    """Snowflake data warehouse configuration."""
    
    account: str = Field(..., env="SNOWFLAKE_ACCOUNT")
    user: str = Field(..., env="SNOWFLAKE_USER")
    password: str = Field(..., env="SNOWFLAKE_PASSWORD")
    role: str = Field(default="STREAMING_ROLE", env="SNOWFLAKE_ROLE")
    warehouse: str = Field(default="STREAMING_WH", env="SNOWFLAKE_WAREHOUSE")
    database: str = Field(default="FINANCIAL_DATA", env="SNOWFLAKE_DATABASE")
    schema: str = Field(default="RAW", env="SNOWFLAKE_SCHEMA")
    
    # Schema organization
    bronze_schema: str = Field(default="BRONZE", env="SNOWFLAKE_BRONZE_SCHEMA")
    silver_schema: str = Field(default="SILVER", env="SNOWFLAKE_SILVER_SCHEMA")
    gold_schema: str = Field(default="GOLD", env="SNOWFLAKE_GOLD_SCHEMA")
    
    # Connection settings
    login_timeout: int = Field(default=60, env="SNOWFLAKE_LOGIN_TIMEOUT")
    network_timeout: int = Field(default=300, env="SNOWFLAKE_NETWORK_TIMEOUT")
    query_timeout: int = Field(default=900, env="SNOWFLAKE_QUERY_TIMEOUT")


class AWSSettings(BaseSettings):
    """AWS configuration for production deployment."""
    
    region: str = Field(default="us-west-2", env="AWS_REGION")
    access_key_id: Optional[str] = Field(default=None, env="AWS_ACCESS_KEY_ID")
    secret_access_key: Optional[str] = Field(default=None, env="AWS_SECRET_ACCESS_KEY")
    
    # S3 Configuration
    s3_bucket_raw: str = Field(default="financial-streaming-raw", env="S3_BUCKET_RAW")
    s3_bucket_processed: str = Field(default="financial-streaming-processed", env="S3_BUCKET_PROCESSED")
    s3_checkpoint_bucket: str = Field(default="financial-streaming-checkpoints", env="S3_CHECKPOINT_BUCKET")
    
    # MSK Configuration
    msk_cluster_arn: Optional[str] = Field(default=None, env="MSK_CLUSTER_ARN")
    msk_bootstrap_servers: Optional[str] = Field(default=None, env="MSK_BOOTSTRAP_SERVERS")


class SparkSettings(BaseSettings):
    """Spark configuration settings."""
    
    app_name: str = Field(default="financial-transaction-processor", env="SPARK_APP_NAME")
    master: str = Field(default="local[*]", env="SPARK_MASTER")
    executor_memory: str = Field(default="2g", env="SPARK_EXECUTOR_MEMORY")
    driver_memory: str = Field(default="1g", env="SPARK_DRIVER_MEMORY")
    max_cores: int = Field(default=4, env="SPARK_MAX_CORES")
    
    # Streaming Configuration
    checkpoint_location: str = Field(
        default="./checkpoints/spark-streaming", 
        env="SPARK_STREAMING_CHECKPOINT_LOCATION"
    )
    trigger_interval: str = Field(default="30 seconds", env="SPARK_STREAMING_TRIGGER_INTERVAL")
    max_offsets_per_trigger: int = Field(default=10000, env="SPARK_STREAMING_MAX_OFFSETS_PER_TRIGGER")
    watermark_delay: str = Field(default="2 hours", env="SPARK_STREAMING_WATERMARK_DELAY")
    
    # Dynamic allocation
    dynamic_allocation_enabled: bool = Field(default=True, env="SPARK_DYNAMIC_ALLOCATION_ENABLED")
    dynamic_allocation_min_executors: int = Field(default=1, env="SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS")
    dynamic_allocation_max_executors: int = Field(default=10, env="SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS")


class SecuritySettings(BaseSettings):
    """Security and encryption settings."""
    
    encryption_key_id: str = Field(default="alias/financial-streaming-key", env="ENCRYPTION_KEY_ID")
    pii_masking_enabled: bool = Field(default=True, env="PII_MASKING_ENABLED")
    audit_logging_enabled: bool = Field(default=True, env="AUDIT_LOGGING_ENABLED")
    
    # Hashing configuration
    customer_id_salt: str = Field(..., env="CUSTOMER_ID_SALT")
    hash_algorithm: str = Field(default="SHA256", env="HASH_ALGORITHM")


class MonitoringSettings(BaseSettings):
    """Monitoring and observability settings."""
    
    prometheus_pushgateway_url: str = Field(
        default="http://localhost:9091", 
        env="PROMETHEUS_PUSHGATEWAY_URL"
    )
    metrics_enabled: bool = Field(default=True, env="METRICS_ENABLED")
    structured_logging: bool = Field(default=True, env="STRUCTURED_LOGGING")
    
    # Alerting
    slack_webhook_url: Optional[str] = Field(default=None, env="SLACK_WEBHOOK_URL")
    email_alerts_enabled: bool = Field(default=False, env="EMAIL_ALERTS_ENABLED")
    alert_email_recipients: str = Field(default="alerts@yourcompany.com", env="ALERT_EMAIL_RECIPIENTS")


class DataQualitySettings(BaseSettings):
    """Data quality and validation settings."""
    
    great_expectations_config_dir: str = Field(
        default="./great_expectations", 
        env="GREAT_EXPECTATIONS_CONFIG_DIR"
    )
    alert_threshold: float = Field(default=0.95, env="DATA_QUALITY_ALERT_THRESHOLD")
    validation_enabled: bool = Field(default=True, env="ENABLE_DATA_VALIDATION")


class FeatureEngineeringSettings(BaseSettings):
    """Feature engineering configuration for fraud detection."""
    
    velocity_window_hours: List[int] = Field(default=[1, 6, 24], env="VELOCITY_WINDOW_HOURS")
    geographic_anomaly_threshold_km: float = Field(default=100.0, env="GEOGRAPHIC_ANOMALY_THRESHOLD")
    amount_anomaly_std_threshold: float = Field(default=3.0, env="AMOUNT_ANOMALY_STD_THRESHOLD")
    merchant_risk_score_threshold: float = Field(default=0.7, env="MERCHANT_RISK_SCORE_THRESHOLD")
    
    @validator('velocity_window_hours', pre=True)
    def parse_velocity_windows(cls, v):
        """Parse velocity windows from comma-separated string or list."""
        if isinstance(v, str):
            return [int(x.strip()) for x in v.split(',')]
        return v


class TransactionGeneratorSettings(BaseSettings):
    """Settings for the transaction data generator."""
    
    transaction_rate_per_second: int = Field(default=100, env="TRANSACTION_RATE_PER_SECOND")
    fraud_injection_rate: float = Field(default=0.05, env="FRAUD_INJECTION_RATE")
    simulate_late_arrivals: bool = Field(default=True, env="SIMULATE_LATE_ARRIVALS")
    late_arrival_probability: float = Field(default=0.02, env="LATE_ARRIVAL_PROBABILITY")


class AppSettings(BaseSettings):
    """Main application settings."""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # Environment
    environment: str = Field(default="development", env="ENVIRONMENT")
    debug_mode: bool = Field(default=False, env="DEBUG_MODE")
    logging_level: str = Field(default="INFO", env="LOGGING_LEVEL")
    
    # Component settings
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    snowflake: SnowflakeSettings = Field(default_factory=SnowflakeSettings)
    aws: AWSSettings = Field(default_factory=AWSSettings)
    spark: SparkSettings = Field(default_factory=SparkSettings)
    security: SecuritySettings = Field(default_factory=SecuritySettings)
    monitoring: MonitoringSettings = Field(default_factory=MonitoringSettings)
    data_quality: DataQualitySettings = Field(default_factory=DataQualitySettings)
    feature_engineering: FeatureEngineeringSettings = Field(default_factory=FeatureEngineeringSettings)
    transaction_generator: TransactionGeneratorSettings = Field(default_factory=TransactionGeneratorSettings)
    
    @property
    def is_production(self) -> bool:
        """Check if running in production environment."""
        return self.environment.lower() == "production"
    
    @property
    def is_development(self) -> bool:
        """Check if running in development environment."""
        return self.environment.lower() == "development"
    
    def get_kafka_config(self) -> Dict[str, Any]:
        """Get Kafka consumer/producer configuration."""
        config = {
            'bootstrap.servers': self.kafka.bootstrap_servers,
            'security.protocol': self.kafka.security_protocol,
            'auto.offset.reset': self.kafka.auto_offset_reset,
            'enable.auto.commit': self.kafka.enable_auto_commit,
            'session.timeout.ms': self.kafka.session_timeout_ms,
        }
        
        if self.kafka.sasl_username and self.kafka.sasl_password:
            config.update({
                'sasl.mechanism': self.kafka.sasl_mechanism,
                'sasl.username': self.kafka.sasl_username,
                'sasl.password': self.kafka.sasl_password,
            })
        
        return config
    
    def get_snowflake_config(self) -> Dict[str, Any]:
        """Get Snowflake connection configuration."""
        return {
            'account': self.snowflake.account,
            'user': self.snowflake.user,
            'password': self.snowflake.password,
            'role': self.snowflake.role,
            'warehouse': self.snowflake.warehouse,
            'database': self.snowflake.database,
            'schema': self.snowflake.schema,
            'login_timeout': self.snowflake.login_timeout,
            'network_timeout': self.snowflake.network_timeout,
            'query_timeout': self.snowflake.query_timeout,
        }


# Global settings instance
settings = AppSettings()
