"""
Configuration settings for the Financial Transaction Streaming Pipeline.
Handles environment variables and application configuration.
"""
from typing import List, Optional, Dict, Any
from pydantic import Field, field_validator, AliasChoices
from pydantic_settings import BaseSettings, SettingsConfigDict


class KafkaSettings(BaseSettings):
    """Kafka configuration settings."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    bootstrap_servers: str = Field(
        default="localhost:9092",
        validation_alias=AliasChoices('KAFKA_BOOTSTRAP_SERVERS', 'kafka_bootstrap_servers')
    )
    security_protocol: str = Field(
        default="PLAINTEXT",
        validation_alias=AliasChoices('KAFKA_SECURITY_PROTOCOL', 'kafka_security_protocol')
    )
    sasl_mechanism: str = Field(
        default="PLAIN",
        validation_alias=AliasChoices('KAFKA_SASL_MECHANISM', 'kafka_sasl_mechanism')
    )
    sasl_username: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices('KAFKA_SASL_USERNAME', 'kafka_sasl_username')
    )
    sasl_password: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices('KAFKA_SASL_PASSWORD', 'kafka_sasl_password')
    )

    # Schema Registry
    schema_registry_url: str = Field(
        default="http://localhost:8081",
        validation_alias=AliasChoices('SCHEMA_REGISTRY_URL', 'schema_registry_url')
    )
    schema_registry_username: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices('SCHEMA_REGISTRY_USERNAME', 'schema_registry_username')
    )
    schema_registry_password: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices('SCHEMA_REGISTRY_PASSWORD', 'schema_registry_password')
    )

    # Topics
    transactions_topic: str = Field(
        default="financial.transactions.raw",
        validation_alias=AliasChoices('TRANSACTIONS_TOPIC', 'transactions_topic')
    )
    fraud_alerts_topic: str = Field(
        default="financial.fraud.alerts",
        validation_alias=AliasChoices('FRAUD_ALERTS_TOPIC', 'fraud_alerts_topic')
    )
    dlq_topic: str = Field(
        default="financial.transactions.dlq",
        validation_alias=AliasChoices('DLQ_TOPIC', 'dlq_topic')
    )

    # Consumer settings
    auto_offset_reset: str = Field(
        default="earliest",
        validation_alias=AliasChoices('KAFKA_AUTO_OFFSET_RESET', 'kafka_auto_offset_reset')
    )
    enable_auto_commit: bool = Field(
        default=True,
        validation_alias=AliasChoices('KAFKA_ENABLE_AUTO_COMMIT', 'kafka_enable_auto_commit')
    )
    session_timeout_ms: int = Field(
        default=30000,
        validation_alias=AliasChoices('KAFKA_SESSION_TIMEOUT_MS', 'kafka_session_timeout_ms')
    )


class SnowflakeSettings(BaseSettings):
    """Snowflake data warehouse configuration."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    account: str = Field(
        default="",
        validation_alias=AliasChoices('SNOWFLAKE_ACCOUNT', 'snowflake_account')
    )
    user: str = Field(
        default="",
        validation_alias=AliasChoices('SNOWFLAKE_USER', 'snowflake_user')
    )
    password: str = Field(
        default="",
        validation_alias=AliasChoices('SNOWFLAKE_PASSWORD', 'snowflake_password')
    )
    role: str = Field(
        default="STREAMING_ROLE",
        validation_alias=AliasChoices('SNOWFLAKE_ROLE', 'snowflake_role')
    )
    warehouse: str = Field(
        default="STREAMING_WH",
        validation_alias=AliasChoices('SNOWFLAKE_WAREHOUSE', 'snowflake_warehouse')
    )
    database: str = Field(
        default="FINANCIAL_DATA",
        validation_alias=AliasChoices('SNOWFLAKE_DATABASE', 'snowflake_database')
    )
    schema_name: str = Field(
        default="RAW",
        validation_alias=AliasChoices('SNOWFLAKE_SCHEMA', 'snowflake_schema')
    )

    # Schema organization
    bronze_schema: str = Field(
        default="BRONZE",
        validation_alias=AliasChoices('SNOWFLAKE_BRONZE_SCHEMA', 'snowflake_bronze_schema')
    )
    silver_schema: str = Field(
        default="SILVER",
        validation_alias=AliasChoices('SNOWFLAKE_SILVER_SCHEMA', 'snowflake_silver_schema')
    )
    gold_schema: str = Field(
        default="GOLD",
        validation_alias=AliasChoices('SNOWFLAKE_GOLD_SCHEMA', 'snowflake_gold_schema')
    )

    # Connection settings
    login_timeout: int = Field(
        default=60,
        validation_alias=AliasChoices('SNOWFLAKE_LOGIN_TIMEOUT', 'snowflake_login_timeout')
    )
    network_timeout: int = Field(
        default=300,
        validation_alias=AliasChoices('SNOWFLAKE_NETWORK_TIMEOUT', 'snowflake_network_timeout')
    )
    query_timeout: int = Field(
        default=900,
        validation_alias=AliasChoices('SNOWFLAKE_QUERY_TIMEOUT', 'snowflake_query_timeout')
    )

    @property
    def schema(self) -> str:
        """Alias for schema_name for backward compatibility."""
        return self.schema_name


class AWSSettings(BaseSettings):
    """AWS configuration for production deployment."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    region: str = Field(
        default="us-west-2",
        validation_alias=AliasChoices('AWS_REGION', 'aws_region')
    )
    access_key_id: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices('AWS_ACCESS_KEY_ID', 'aws_access_key_id')
    )
    secret_access_key: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices('AWS_SECRET_ACCESS_KEY', 'aws_secret_access_key')
    )

    # S3 Configuration
    s3_bucket_raw: str = Field(
        default="financial-streaming-raw",
        validation_alias=AliasChoices('S3_BUCKET_RAW', 's3_bucket_raw')
    )
    s3_bucket_processed: str = Field(
        default="financial-streaming-processed",
        validation_alias=AliasChoices('S3_BUCKET_PROCESSED', 's3_bucket_processed')
    )
    s3_checkpoint_bucket: str = Field(
        default="financial-streaming-checkpoints",
        validation_alias=AliasChoices('S3_CHECKPOINT_BUCKET', 's3_checkpoint_bucket')
    )

    # MSK Configuration
    msk_cluster_arn: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices('MSK_CLUSTER_ARN', 'msk_cluster_arn')
    )
    msk_bootstrap_servers: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices('MSK_BOOTSTRAP_SERVERS', 'msk_bootstrap_servers')
    )


class SparkSettings(BaseSettings):
    """Spark configuration settings."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    app_name: str = Field(
        default="financial-transaction-processor",
        validation_alias=AliasChoices('SPARK_APP_NAME', 'spark_app_name')
    )
    master: str = Field(
        default="local[*]",
        validation_alias=AliasChoices('SPARK_MASTER', 'spark_master')
    )
    executor_memory: str = Field(
        default="2g",
        validation_alias=AliasChoices('SPARK_EXECUTOR_MEMORY', 'spark_executor_memory')
    )
    driver_memory: str = Field(
        default="1g",
        validation_alias=AliasChoices('SPARK_DRIVER_MEMORY', 'spark_driver_memory')
    )
    max_cores: int = Field(
        default=4,
        validation_alias=AliasChoices('SPARK_MAX_CORES', 'spark_max_cores')
    )

    # Streaming Configuration
    checkpoint_location: str = Field(
        default="./checkpoints/spark-streaming",
        validation_alias=AliasChoices('SPARK_STREAMING_CHECKPOINT_LOCATION', 'spark_streaming_checkpoint_location')
    )
    trigger_interval: str = Field(
        default="30 seconds",
        validation_alias=AliasChoices('SPARK_STREAMING_TRIGGER_INTERVAL', 'spark_streaming_trigger_interval')
    )
    max_offsets_per_trigger: int = Field(
        default=10000,
        validation_alias=AliasChoices('SPARK_STREAMING_MAX_OFFSETS_PER_TRIGGER', 'spark_streaming_max_offsets_per_trigger')
    )
    watermark_delay: str = Field(
        default="2 hours",
        validation_alias=AliasChoices('SPARK_STREAMING_WATERMARK_DELAY', 'spark_streaming_watermark_delay')
    )

    # Dynamic allocation
    dynamic_allocation_enabled: bool = Field(
        default=True,
        validation_alias=AliasChoices('SPARK_DYNAMIC_ALLOCATION_ENABLED', 'spark_dynamic_allocation_enabled')
    )
    dynamic_allocation_min_executors: int = Field(
        default=1,
        validation_alias=AliasChoices('SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS', 'spark_dynamic_allocation_min_executors')
    )
    dynamic_allocation_max_executors: int = Field(
        default=10,
        validation_alias=AliasChoices('SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS', 'spark_dynamic_allocation_max_executors')
    )


class SecuritySettings(BaseSettings):
    """Security and encryption settings."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    encryption_key_id: str = Field(
        default="alias/financial-streaming-key",
        validation_alias=AliasChoices('ENCRYPTION_KEY_ID', 'encryption_key_id')
    )
    pii_masking_enabled: bool = Field(
        default=True,
        validation_alias=AliasChoices('PII_MASKING_ENABLED', 'pii_masking_enabled')
    )
    audit_logging_enabled: bool = Field(
        default=True,
        validation_alias=AliasChoices('AUDIT_LOGGING_ENABLED', 'audit_logging_enabled')
    )

    # Hashing configuration
    customer_id_salt: str = Field(
        default="default-salt-change-in-production",
        validation_alias=AliasChoices('CUSTOMER_ID_SALT', 'customer_id_salt')
    )
    hash_algorithm: str = Field(
        default="SHA256",
        validation_alias=AliasChoices('HASH_ALGORITHM', 'hash_algorithm')
    )

    # Hash length for identifiers (characters)
    hash_length: int = Field(
        default=16,
        validation_alias=AliasChoices('HASH_LENGTH', 'hash_length')
    )


class MonitoringSettings(BaseSettings):
    """Monitoring and observability settings."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    prometheus_pushgateway_url: str = Field(
        default="http://localhost:9091",
        validation_alias=AliasChoices('PROMETHEUS_PUSHGATEWAY_URL', 'prometheus_pushgateway_url')
    )
    metrics_enabled: bool = Field(
        default=True,
        validation_alias=AliasChoices('METRICS_ENABLED', 'metrics_enabled')
    )
    structured_logging: bool = Field(
        default=True,
        validation_alias=AliasChoices('STRUCTURED_LOGGING', 'structured_logging')
    )

    # Alerting
    slack_webhook_url: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices('SLACK_WEBHOOK_URL', 'slack_webhook_url')
    )
    email_alerts_enabled: bool = Field(
        default=False,
        validation_alias=AliasChoices('EMAIL_ALERTS_ENABLED', 'email_alerts_enabled')
    )
    alert_email_recipients: str = Field(
        default="alerts@yourcompany.com",
        validation_alias=AliasChoices('ALERT_EMAIL_RECIPIENTS', 'alert_email_recipients')
    )


class DataQualitySettings(BaseSettings):
    """Data quality and validation settings."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    great_expectations_config_dir: str = Field(
        default="./great_expectations",
        validation_alias=AliasChoices('GREAT_EXPECTATIONS_CONFIG_DIR', 'great_expectations_config_dir')
    )
    alert_threshold: float = Field(
        default=0.95,
        validation_alias=AliasChoices('DATA_QUALITY_ALERT_THRESHOLD', 'data_quality_alert_threshold')
    )
    validation_enabled: bool = Field(
        default=True,
        validation_alias=AliasChoices('ENABLE_DATA_VALIDATION', 'enable_data_validation')
    )

    # Silver layer data quality threshold
    silver_quality_threshold: float = Field(
        default=0.8,
        validation_alias=AliasChoices('SILVER_QUALITY_THRESHOLD', 'silver_quality_threshold')
    )


class FeatureEngineeringSettings(BaseSettings):
    """Feature engineering configuration for fraud detection."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    velocity_window_hours: List[int] = Field(
        default=[1, 6, 24],
        validation_alias=AliasChoices('VELOCITY_WINDOW_HOURS', 'velocity_window_hours')
    )
    geographic_anomaly_threshold_km: float = Field(
        default=100.0,
        validation_alias=AliasChoices('GEOGRAPHIC_ANOMALY_THRESHOLD', 'geographic_anomaly_threshold')
    )
    amount_anomaly_std_threshold: float = Field(
        default=3.0,
        validation_alias=AliasChoices('AMOUNT_ANOMALY_STD_THRESHOLD', 'amount_anomaly_std_threshold')
    )
    merchant_risk_score_threshold: float = Field(
        default=0.7,
        validation_alias=AliasChoices('MERCHANT_RISK_SCORE_THRESHOLD', 'merchant_risk_score_threshold')
    )

    @field_validator('velocity_window_hours', mode='before')
    @classmethod
    def parse_velocity_windows(cls, v):
        """Parse velocity windows from comma-separated string or list."""
        if isinstance(v, str):
            return [int(x.strip()) for x in v.split(',')]
        return v


class TransactionGeneratorSettings(BaseSettings):
    """Settings for the transaction data generator."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    transaction_rate_per_second: int = Field(
        default=100,
        validation_alias=AliasChoices('TRANSACTION_RATE_PER_SECOND', 'transaction_rate_per_second')
    )
    fraud_injection_rate: float = Field(
        default=0.05,
        validation_alias=AliasChoices('FRAUD_INJECTION_RATE', 'fraud_injection_rate')
    )
    simulate_late_arrivals: bool = Field(
        default=True,
        validation_alias=AliasChoices('SIMULATE_LATE_ARRIVALS', 'simulate_late_arrivals')
    )
    late_arrival_probability: float = Field(
        default=0.02,
        validation_alias=AliasChoices('LATE_ARRIVAL_PROBABILITY', 'late_arrival_probability')
    )


class AppSettings(BaseSettings):
    """Main application settings."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )

    # Environment
    environment: str = Field(
        default="development",
        validation_alias=AliasChoices('ENVIRONMENT', 'environment')
    )
    debug_mode: bool = Field(
        default=False,
        validation_alias=AliasChoices('DEBUG_MODE', 'debug_mode')
    )
    logging_level: str = Field(
        default="INFO",
        validation_alias=AliasChoices('LOGGING_LEVEL', 'logging_level')
    )

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


def get_settings() -> AppSettings:
    """
    Factory function to get settings instance.
    Useful for dependency injection and testing.
    """
    return AppSettings()


# Global settings instance (for backward compatibility)
settings = get_settings()
