"""
Unit tests for data quality validation module.
"""
import pytest
from datetime import datetime, timezone, timedelta
from decimal import Decimal


class TestDataQualityValidator:
    """Tests for DataQualityValidator class."""

    def test_validate_transaction_valid(self, sample_transaction):
        """Test validation of a valid transaction."""
        from src.utils.data_quality import DataQualityValidator
        validator = DataQualityValidator()

        is_valid, errors = validator.validate_transaction(sample_transaction)

        assert is_valid is True
        assert len(errors) == 0

    def test_validate_transaction_missing_required_field(self):
        """Test validation fails with missing required field."""
        from src.utils.data_quality import DataQualityValidator
        validator = DataQualityValidator()

        invalid_txn = {
            "transaction_id": "test-123",
            # Missing customer_id, amount, etc.
        }

        is_valid, errors = validator.validate_transaction(invalid_txn)

        assert is_valid is False
        assert len(errors) > 0
        assert any("required" in e.lower() for e in errors)

    def test_validate_transaction_empty_required_field(self):
        """Test validation fails with empty required field."""
        from src.utils.data_quality import DataQualityValidator
        validator = DataQualityValidator()

        invalid_txn = {
            "transaction_id": "test-123",
            "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
            "customer_id": "",  # Empty string
            "amount": "100.00",
            "currency": "USD",
            "merchant_id": "merch_123",
            "status": "APPROVED"
        }

        is_valid, errors = validator.validate_transaction(invalid_txn)

        assert is_valid is False
        assert any("empty" in e.lower() for e in errors)

    def test_validate_transaction_invalid_amount_negative(self):
        """Test validation fails with negative amount."""
        from src.utils.data_quality import DataQualityValidator
        validator = DataQualityValidator()

        invalid_txn = {
            "transaction_id": "550e8400-e29b-41d4-a716-446655440000",
            "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
            "customer_id": "cust_123",
            "amount": "-50.00",
            "currency": "USD",
            "merchant_id": "merch_123",
            "status": "APPROVED"
        }

        is_valid, errors = validator.validate_transaction(invalid_txn)

        assert is_valid is False
        assert any("amount" in e.lower() for e in errors)

    def test_validate_transaction_amount_too_high(self):
        """Test validation fails with amount exceeding maximum."""
        from src.utils.data_quality import DataQualityValidator
        validator = DataQualityValidator()

        invalid_txn = {
            "transaction_id": "550e8400-e29b-41d4-a716-446655440000",
            "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
            "customer_id": "cust_123",
            "amount": "100000.00",  # Way over max
            "currency": "USD",
            "merchant_id": "merch_123",
            "status": "APPROVED"
        }

        is_valid, errors = validator.validate_transaction(invalid_txn)

        assert is_valid is False
        assert any("amount" in e.lower() or "exceeds" in e.lower() for e in errors)

    def test_validate_transaction_invalid_status(self):
        """Test validation fails with invalid status."""
        from src.utils.data_quality import DataQualityValidator
        validator = DataQualityValidator()

        invalid_txn = {
            "transaction_id": "550e8400-e29b-41d4-a716-446655440000",
            "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
            "customer_id": "cust_123",
            "amount": "100.00",
            "currency": "USD",
            "merchant_id": "merch_123",
            "status": "INVALID_STATUS"
        }

        is_valid, errors = validator.validate_transaction(invalid_txn)

        assert is_valid is False
        assert any("status" in e.lower() for e in errors)

    def test_validate_transaction_invalid_currency(self):
        """Test validation fails with invalid currency."""
        from src.utils.data_quality import DataQualityValidator
        validator = DataQualityValidator()

        invalid_txn = {
            "transaction_id": "550e8400-e29b-41d4-a716-446655440000",
            "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
            "customer_id": "cust_123",
            "amount": "100.00",
            "currency": "INVALID",
            "merchant_id": "merch_123",
            "status": "APPROVED"
        }

        is_valid, errors = validator.validate_transaction(invalid_txn)

        assert is_valid is False
        assert any("currency" in e.lower() for e in errors)

    def test_validate_transaction_future_timestamp(self):
        """Test validation fails with timestamp too far in future."""
        from src.utils.data_quality import DataQualityValidator
        validator = DataQualityValidator()

        future_time = datetime.now(timezone.utc) + timedelta(hours=5)
        invalid_txn = {
            "transaction_id": "550e8400-e29b-41d4-a716-446655440000",
            "timestamp": int(future_time.timestamp() * 1000),
            "customer_id": "cust_123",
            "amount": "100.00",
            "currency": "USD",
            "merchant_id": "merch_123",
            "status": "APPROVED"
        }

        is_valid, errors = validator.validate_transaction(invalid_txn)

        assert is_valid is False
        assert any("future" in e.lower() or "timestamp" in e.lower() for e in errors)

    def test_validate_fraud_alert_valid(self, sample_fraud_alert):
        """Test validation of valid fraud alert."""
        from src.utils.data_quality import DataQualityValidator
        validator = DataQualityValidator()

        is_valid, errors = validator.validate_fraud_alert(sample_fraud_alert)

        assert is_valid is True
        assert len(errors) == 0

    def test_validate_fraud_alert_missing_field(self):
        """Test fraud alert validation with missing field."""
        from src.utils.data_quality import DataQualityValidator
        validator = DataQualityValidator()

        invalid_alert = {
            "alert_id": "alert_123",
            # Missing required fields
        }

        is_valid, errors = validator.validate_fraud_alert(invalid_alert)

        assert is_valid is False
        assert len(errors) > 0

    def test_validate_fraud_alert_invalid_risk_score(self):
        """Test fraud alert validation with invalid risk score."""
        from src.utils.data_quality import DataQualityValidator
        validator = DataQualityValidator()

        invalid_alert = {
            "alert_id": "alert_123",
            "transaction_id": "txn_123",
            "customer_id_hash": "abc123",
            "alert_type": "VELOCITY",
            "severity": "HIGH",
            "risk_score": 1.5  # Out of range [0, 1]
        }

        is_valid, errors = validator.validate_fraud_alert(invalid_alert)

        assert is_valid is False
        assert any("risk_score" in e.lower() for e in errors)


class TestDataQualityValidatorBatch:
    """Tests for batch validation functionality."""

    def test_validate_batch_all_valid(self, batch_transactions):
        """Test batch validation with all valid transactions."""
        from src.utils.data_quality import DataQualityValidator
        validator = DataQualityValidator()

        summary = validator.validate_batch(batch_transactions)

        assert summary["batch_size"] == len(batch_transactions)
        assert summary["valid_count"] == len(batch_transactions)
        assert summary["invalid_count"] == 0
        assert summary["validation_rate"] == 1.0

    def test_validate_batch_mixed_validity(self, mixed_validity_transactions):
        """Test batch validation with mixed valid/invalid transactions."""
        from src.utils.data_quality import DataQualityValidator
        validator = DataQualityValidator()

        summary = validator.validate_batch(mixed_validity_transactions)

        assert summary["batch_size"] == len(mixed_validity_transactions)
        assert summary["valid_count"] < summary["batch_size"]
        assert summary["invalid_count"] > 0
        assert 0 < summary["validation_rate"] < 1.0

    def test_validate_batch_empty(self):
        """Test batch validation with empty list."""
        from src.utils.data_quality import DataQualityValidator
        validator = DataQualityValidator()

        summary = validator.validate_batch([])

        assert summary["batch_size"] == 0
        assert summary["validation_rate"] == 0

    def test_validate_batch_includes_processing_time(self, batch_transactions):
        """Test that batch validation includes processing time."""
        from src.utils.data_quality import DataQualityValidator
        validator = DataQualityValidator()

        summary = validator.validate_batch(batch_transactions)

        assert "processing_time_seconds" in summary
        assert summary["processing_time_seconds"] >= 0


class TestDataQualityScore:
    """Tests for data quality scoring."""

    def test_get_data_quality_score_perfect(self, batch_transactions):
        """Test quality score with all valid records."""
        from src.utils.data_quality import DataQualityValidator
        validator = DataQualityValidator()

        summary = validator.validate_batch(batch_transactions)
        score = validator.get_data_quality_score(summary)

        assert score == 1.0

    def test_get_data_quality_score_empty_batch(self):
        """Test quality score with empty batch."""
        from src.utils.data_quality import DataQualityValidator
        validator = DataQualityValidator()

        summary = {"batch_size": 0, "validation_rate": 0, "invalid_count": 0}
        score = validator.get_data_quality_score(summary)

        assert score == 0.0


class TestDataQualityReport:
    """Tests for data quality report generation."""

    def test_generate_report_no_data(self):
        """Test report generation with no validations."""
        from src.utils.data_quality import DataQualityValidator
        validator = DataQualityValidator()
        validator.reset_stats()

        report = validator.generate_data_quality_report()

        assert report["status"] == "NO_DATA"

    def test_generate_report_healthy(self, batch_transactions):
        """Test report generation with healthy data."""
        from src.utils.data_quality import DataQualityValidator
        validator = DataQualityValidator()
        validator.reset_stats()

        # Validate batch to populate stats
        validator.validate_batch(batch_transactions)
        report = validator.generate_data_quality_report()

        assert report["status"] == "HEALTHY"
        assert "overall_statistics" in report
        assert "quality_score" in report
        assert "recommendations" in report

    def test_reset_stats(self):
        """Test that stats can be reset."""
        from src.utils.data_quality import DataQualityValidator
        validator = DataQualityValidator()

        validator.stats["total_validations"] = 100
        validator.reset_stats()

        assert validator.stats["total_validations"] == 0
        assert validator.stats["passed_validations"] == 0


class TestDataQualityMonitor:
    """Tests for DataQualityMonitor class."""

    def test_add_quality_metrics(self, batch_transactions):
        """Test adding quality metrics."""
        from src.utils.data_quality import DataQualityMonitor, DataQualityValidator
        monitor = DataQualityMonitor()
        validator = DataQualityValidator()

        summary = validator.validate_batch(batch_transactions)
        monitor.add_quality_metrics(summary)

        assert len(monitor.quality_history) == 1
        assert "quality_score" in monitor.quality_history[0]

    def test_quality_history_limit(self):
        """Test that quality history is limited to 1000 entries."""
        from src.utils.data_quality import DataQualityMonitor
        monitor = DataQualityMonitor()

        # Add more than 1000 entries
        for i in range(1100):
            monitor.quality_history.append({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "quality_score": 0.95
            })

        # Trigger cleanup by adding metrics
        summary = {
            "batch_size": 10,
            "validation_rate": 0.9,
            "processing_time_seconds": 0.1,
            "invalid_count": 1
        }
        monitor.add_quality_metrics(summary)

        assert len(monitor.quality_history) <= 1001

    def test_get_quality_trend_no_data(self):
        """Test getting trend with no data."""
        from src.utils.data_quality import DataQualityMonitor
        monitor = DataQualityMonitor()

        trend = monitor.get_quality_trend(hours=24)

        assert trend["trend"] == "NO_DATA"

    def test_check_quality_alerts_no_history(self):
        """Test alerts check with no history."""
        from src.utils.data_quality import DataQualityMonitor
        monitor = DataQualityMonitor()

        alerts = monitor.check_quality_alerts()

        assert alerts == []
