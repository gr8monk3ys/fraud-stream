"""
Unit tests for configuration settings module.
"""
import os
import pytest
from unittest.mock import patch


class TestKafkaSettings:
    """Tests for Kafka configuration settings."""

    def test_default_bootstrap_servers(self):
        """Test default Kafka bootstrap servers."""
        from src.config.settings import KafkaSettings
        settings = KafkaSettings()
        assert settings.bootstrap_servers == "localhost:9092"

    def test_default_schema_registry_url(self):
        """Test default Schema Registry URL."""
        from src.config.settings import KafkaSettings
        settings = KafkaSettings()
        assert settings.schema_registry_url == "http://localhost:8081"

    def test_default_topics(self):
        """Test default Kafka topic names."""
        from src.config.settings import KafkaSettings
        settings = KafkaSettings()
        assert settings.transactions_topic == "financial.transactions.raw"
        assert settings.fraud_alerts_topic == "financial.fraud.alerts"
        assert settings.dlq_topic == "financial.transactions.dlq"

    def test_env_override(self):
        """Test environment variable override."""
        with patch.dict(os.environ, {"KAFKA_BOOTSTRAP_SERVERS": "kafka:9093"}):
            from src.config.settings import KafkaSettings
            settings = KafkaSettings()
            assert settings.bootstrap_servers == "kafka:9093"


class TestSnowflakeSettings:
    """Tests for Snowflake configuration settings."""

    def test_default_warehouse(self):
        """Test default Snowflake warehouse name."""
        from src.config.settings import SnowflakeSettings
        settings = SnowflakeSettings()
        assert settings.warehouse == "STREAMING_WH"

    def test_default_schemas(self):
        """Test default Snowflake schema names."""
        from src.config.settings import SnowflakeSettings
        settings = SnowflakeSettings()
        assert settings.bronze_schema == "BRONZE"
        assert settings.silver_schema == "SILVER"
        assert settings.gold_schema == "GOLD"

    def test_schema_property(self):
        """Test schema property for backward compatibility."""
        from src.config.settings import SnowflakeSettings
        settings = SnowflakeSettings()
        assert settings.schema == settings.schema_name


class TestSecuritySettings:
    """Tests for security configuration settings."""

    def test_default_pii_masking_enabled(self):
        """Test PII masking is enabled by default."""
        from src.config.settings import SecuritySettings
        settings = SecuritySettings()
        assert settings.pii_masking_enabled is True

    def test_default_audit_logging_enabled(self):
        """Test audit logging is enabled by default."""
        from src.config.settings import SecuritySettings
        settings = SecuritySettings()
        assert settings.audit_logging_enabled is True

    def test_hash_length_setting(self):
        """Test hash length configuration."""
        from src.config.settings import SecuritySettings
        settings = SecuritySettings()
        assert settings.hash_length == 16


class TestDataQualitySettings:
    """Tests for data quality configuration settings."""

    def test_default_alert_threshold(self):
        """Test default data quality alert threshold."""
        from src.config.settings import DataQualitySettings
        settings = DataQualitySettings()
        assert settings.alert_threshold == 0.95

    def test_silver_quality_threshold(self):
        """Test silver layer quality threshold."""
        from src.config.settings import DataQualitySettings
        settings = DataQualitySettings()
        assert settings.silver_quality_threshold == 0.8


class TestFeatureEngineeringSettings:
    """Tests for feature engineering configuration settings."""

    def test_default_velocity_windows(self):
        """Test default velocity window hours."""
        from src.config.settings import FeatureEngineeringSettings
        settings = FeatureEngineeringSettings()
        assert settings.velocity_window_hours == [1, 6, 24]

    def test_velocity_windows_from_string(self):
        """Test parsing velocity windows from comma-separated string."""
        with patch.dict(os.environ, {"VELOCITY_WINDOW_HOURS": "2,12,48"}):
            from src.config.settings import FeatureEngineeringSettings
            settings = FeatureEngineeringSettings()
            assert settings.velocity_window_hours == [2, 12, 48]

    def test_default_anomaly_threshold(self):
        """Test default amount anomaly threshold."""
        from src.config.settings import FeatureEngineeringSettings
        settings = FeatureEngineeringSettings()
        assert settings.amount_anomaly_std_threshold == 3.0


class TestAppSettings:
    """Tests for main application settings."""

    def test_default_environment(self):
        """Test default environment is development."""
        from src.config.settings import AppSettings
        settings = AppSettings()
        # May be 'test' due to conftest.py
        assert settings.environment in ["development", "test"]

    def test_is_production_property(self):
        """Test is_production property."""
        from src.config.settings import AppSettings
        with patch.dict(os.environ, {"ENVIRONMENT": "production"}):
            settings = AppSettings()
            assert settings.is_production is True

    def test_is_development_property(self):
        """Test is_development property."""
        from src.config.settings import AppSettings
        with patch.dict(os.environ, {"ENVIRONMENT": "development"}):
            settings = AppSettings()
            assert settings.is_development is True

    def test_get_kafka_config(self):
        """Test Kafka configuration dictionary generation."""
        from src.config.settings import AppSettings
        settings = AppSettings()
        config = settings.get_kafka_config()

        assert "bootstrap.servers" in config
        assert "security.protocol" in config
        assert "auto.offset.reset" in config

    def test_get_snowflake_config(self):
        """Test Snowflake configuration dictionary generation."""
        from src.config.settings import AppSettings
        settings = AppSettings()
        config = settings.get_snowflake_config()

        assert "account" in config
        assert "warehouse" in config
        assert "database" in config

    def test_nested_settings_instantiation(self):
        """Test that nested settings are properly instantiated."""
        from src.config.settings import AppSettings
        settings = AppSettings()

        assert settings.kafka is not None
        assert settings.snowflake is not None
        assert settings.security is not None
        assert settings.data_quality is not None


class TestGetSettingsFactory:
    """Tests for settings factory function."""

    def test_get_settings_returns_app_settings(self):
        """Test that get_settings returns AppSettings instance."""
        from src.config.settings import get_settings, AppSettings
        settings = get_settings()
        assert isinstance(settings, AppSettings)

    def test_get_settings_creates_new_instance(self):
        """Test that get_settings creates new instance each call."""
        from src.config.settings import get_settings
        settings1 = get_settings()
        settings2 = get_settings()
        # They should be different instances
        assert settings1 is not settings2
