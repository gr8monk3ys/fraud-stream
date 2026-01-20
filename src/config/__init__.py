"""Configuration package for the Financial Transaction Streaming Pipeline."""

from .settings import (
    settings,
    get_settings,
    AppSettings,
    KafkaSettings,
    SnowflakeSettings,
    AWSSettings,
    SparkSettings,
    SecuritySettings,
    MonitoringSettings,
    DataQualitySettings,
    FeatureEngineeringSettings,
    TransactionGeneratorSettings,
)

__all__ = [
    # Global settings instance and factory
    "settings",
    "get_settings",
    # Settings classes
    "AppSettings",
    "KafkaSettings",
    "SnowflakeSettings",
    "AWSSettings",
    "SparkSettings",
    "SecuritySettings",
    "MonitoringSettings",
    "DataQualitySettings",
    "FeatureEngineeringSettings",
    "TransactionGeneratorSettings",
]
