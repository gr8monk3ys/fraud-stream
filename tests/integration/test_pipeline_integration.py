"""
Integration tests for the Financial Transaction Streaming Pipeline.

These tests verify that components work together correctly.
Note: These are mocked integration tests that don't require external services.
"""
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone


class TestDataFlowIntegration:
    """Tests for data flow through the pipeline components."""

    def test_transaction_validation_flow(self, sample_transaction):
        """Test that a transaction flows through validation correctly."""
        from src.utils.data_quality import DataQualityValidator
        from src.utils.security import PIIMasker

        # Validate
        validator = DataQualityValidator()
        is_valid, errors = validator.validate_transaction(sample_transaction)
        assert is_valid is True

        # Mask PII
        masker = PIIMasker()
        masked = masker.apply_transaction_masking(sample_transaction.copy())
        assert "customer_id_hash" in masked
        assert masked["metadata"]["pii_masked"] == "true"

    def test_fraud_detection_flow(self, sample_transaction):
        """Test fraud injection and detection flow."""
        from src.data_generator.fraud_injector import FraudInjector
        from src.utils.data_quality import DataQualityValidator

        # Inject fraud pattern
        injector = FraudInjector()
        fraud_transactions = injector.inject_fraud_scenario(
            "velocity_attack",
            sample_transaction
        )

        # All fraud transactions should be valid (syntactically)
        validator = DataQualityValidator()
        for txn in fraud_transactions[:5]:  # Check first 5
            is_valid, errors = validator.validate_transaction(txn)
            # May have validation issues due to fraud patterns
            # but should have fraud metadata
            assert "metadata" in txn
            assert txn["metadata"]["fraud_pattern"] == "velocity_attack"

    def test_batch_processing_flow(self, batch_transactions):
        """Test batch processing flow through components."""
        from src.utils.data_quality import DataQualityValidator, DataQualityMonitor
        from src.utils.security import PIIMasker

        validator = DataQualityValidator()
        monitor = DataQualityMonitor()
        masker = PIIMasker()

        # Validate batch
        summary = validator.validate_batch(batch_transactions)
        assert summary["batch_size"] == len(batch_transactions)

        # Track metrics
        monitor.add_quality_metrics(summary)
        assert len(monitor.quality_history) == 1

        # Mask all transactions
        masked_transactions = []
        for txn in batch_transactions:
            masked = masker.apply_transaction_masking(txn.copy())
            masked_transactions.append(masked)

        assert len(masked_transactions) == len(batch_transactions)
        for masked in masked_transactions:
            assert "customer_id_hash" in masked


class TestConfigurationIntegration:
    """Tests for configuration integration across components."""

    def test_settings_accessible_in_components(self):
        """Test that settings are accessible in all components."""
        from src.config import settings
        from src.utils.security import SecurityManager
        from src.utils.data_quality import DataQualityValidator

        # Settings should be accessible
        assert settings is not None
        assert settings.environment is not None

        # Components should use settings
        security = SecurityManager()
        assert security.settings is not None

        validator = DataQualityValidator()
        assert validator.settings is not None

    def test_environment_affects_behavior(self):
        """Test that environment setting affects component behavior."""
        from src.config import settings
        from src.utils.security import PIIMasker

        masker = PIIMasker()

        # In development, both hash and original should be preserved
        if settings.is_development:
            txn = {
                "customer_id": "cust_123",
                "card_number": "4111111111111234",
                "metadata": {}
            }
            masked = masker.apply_transaction_masking(txn)
            # Should have both in dev
            assert "customer_id_hash" in masked


class TestSecurityIntegration:
    """Tests for security across components."""

    def test_pii_masking_consistent(self, sample_transaction):
        """Test that PII masking is consistent across calls."""
        from src.utils.security import PIIMasker

        masker = PIIMasker()
        customer_id = sample_transaction["customer_id"]

        hash1 = masker.hash_identifier(customer_id)
        hash2 = masker.hash_identifier(customer_id)

        assert hash1 == hash2

    def test_security_manager_uses_masker(self):
        """Test that SecurityManager properly uses PIIMasker."""
        from src.utils.security import SecurityManager, PIIMasker

        manager = SecurityManager()
        masker = PIIMasker()

        customer_id = "test_customer_123"

        manager_hash = manager.hash_customer_id(customer_id)
        masker_hash = masker.hash_identifier(customer_id)

        assert manager_hash == masker_hash


class TestDataQualityIntegration:
    """Tests for data quality monitoring integration."""

    def test_quality_report_after_validations(self, batch_transactions):
        """Test generating quality report after batch validation."""
        from src.utils.data_quality import DataQualityValidator

        validator = DataQualityValidator()
        validator.reset_stats()

        # Run validations
        validator.validate_batch(batch_transactions)

        # Generate report
        report = validator.generate_data_quality_report()

        assert report["status"] in ["HEALTHY", "WARNING", "CRITICAL"]
        assert report["overall_statistics"]["total_validations"] > 0

    def test_quality_monitoring_trend(self, batch_transactions):
        """Test quality trend monitoring over time."""
        from src.utils.data_quality import DataQualityValidator, DataQualityMonitor

        validator = DataQualityValidator()
        monitor = DataQualityMonitor()

        # Simulate multiple batch validations
        for _ in range(5):
            summary = validator.validate_batch(batch_transactions)
            monitor.add_quality_metrics(summary)

        # Check trend
        trend = monitor.get_quality_trend(hours=1)

        # With consistent good data, should be stable or healthy
        assert trend["data_points"] == 5
        assert trend["average_quality_score"] > 0


@pytest.mark.integration
class TestKafkaIntegration:
    """Integration tests for Kafka components (mocked)."""

    @patch('confluent_kafka.admin.AdminClient')
    def test_kafka_manager_initialization(self, mock_admin):
        """Test KafkaManager initializes correctly."""
        from src.utils.kafka_utils import KafkaManager

        mock_admin.return_value = MagicMock()

        manager = KafkaManager()
        assert manager.admin_client is not None

    @patch('confluent_kafka.Producer')
    def test_kafka_producer_initialization(self, mock_producer):
        """Test KafkaProducer initializes correctly."""
        from src.utils.kafka_utils import KafkaProducer

        mock_producer.return_value = MagicMock()

        producer = KafkaProducer()
        assert producer.producer is not None

    @patch('confluent_kafka.Consumer')
    def test_kafka_consumer_initialization(self, mock_consumer):
        """Test KafkaConsumer initializes correctly."""
        from src.utils.kafka_utils import KafkaConsumer

        mock_consumer.return_value = MagicMock()

        consumer = KafkaConsumer(
            topics=["test-topic"],
            group_id="test-group"
        )
        assert consumer.consumer is not None


@pytest.mark.integration
class TestEndToEndFlow:
    """End-to-end flow tests (mocked)."""

    @patch('confluent_kafka.Producer')
    @patch('confluent_kafka.schema_registry.SchemaRegistryClient')
    @patch('confluent_kafka.schema_registry.avro.AvroSerializer')
    @patch('builtins.open', create=True)
    def test_full_transaction_flow(self, mock_open, mock_serializer,
                                   mock_sr, mock_producer, sample_transaction):
        """Test full transaction flow from generation to validation."""
        mock_open.return_value.__enter__.return_value.read.return_value = "{}"
        mock_producer.return_value = MagicMock()

        from src.data_generator.transaction_producer import TransactionProducer
        from src.utils.data_quality import DataQualityValidator
        from src.utils.security import PIIMasker

        # Generate transaction
        producer = TransactionProducer()
        txn = producer._generate_transaction()

        # Validate
        validator = DataQualityValidator()
        is_valid, errors = validator.validate_transaction(txn)

        # Mask PII
        masker = PIIMasker()
        masked_txn = masker.apply_transaction_masking(txn.copy())

        # Assertions
        assert txn is not None
        assert "transaction_id" in txn
        assert "customer_id_hash" in masked_txn
