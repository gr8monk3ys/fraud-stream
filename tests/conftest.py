"""
Pytest configuration and shared fixtures for the test suite.
"""
import os
import pytest
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, Any
from unittest.mock import MagicMock, patch
import uuid


# Set test environment variables before importing settings
@pytest.fixture(scope="session", autouse=True)
def set_test_environment():
    """Set environment variables for testing."""
    os.environ.setdefault("ENVIRONMENT", "test")
    os.environ.setdefault("CUSTOMER_ID_SALT", "test-salt-for-testing-only")
    os.environ.setdefault("SNOWFLAKE_ACCOUNT", "test_account")
    os.environ.setdefault("SNOWFLAKE_USER", "test_user")
    os.environ.setdefault("SNOWFLAKE_PASSWORD", "test_password")
    yield


@pytest.fixture
def sample_transaction() -> Dict[str, Any]:
    """Create a sample valid transaction for testing."""
    now = datetime.now(timezone.utc)
    return {
        "transaction_id": str(uuid.uuid4()),
        "timestamp": int(now.timestamp() * 1000),
        "customer_id": "cust_abc12345",
        "card_number_last4": "1234",
        "card_type": "CREDIT",
        "amount": "150.50",
        "currency": "USD",
        "merchant_id": "merch_xyz98765",
        "merchant_name": "Test Merchant",
        "merchant_category_code": "5411",
        "merchant_category": "GROCERY",
        "location": {
            "country": "US",
            "state": "CA",
            "city": "San Francisco",
            "zip_code": "94102",
            "latitude": 37.7749,
            "longitude": -122.4194
        },
        "channel": "POS",
        "status": "APPROVED",
        "risk_score": 0.15,
        "authorization_code": "AUTH123456",
        "processor_response_code": "00",
        "is_international": False,
        "customer_present": True,
        "entry_method": "CHIP",
        "metadata": {
            "customer_spending_pattern": "moderate",
            "merchant_risk_score": "0.2"
        },
        "created_at": int(now.timestamp() * 1000),
        "version": "1.0"
    }


@pytest.fixture
def sample_invalid_transaction() -> Dict[str, Any]:
    """Create an invalid transaction for testing validation."""
    return {
        "transaction_id": "invalid-not-uuid",
        "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
        "customer_id": "",  # Empty - invalid
        "amount": "-100",  # Negative - invalid
        "currency": "INVALID",  # Invalid currency
        "merchant_id": "merch_123",
        "status": "UNKNOWN"  # Invalid status
    }


@pytest.fixture
def sample_fraud_alert() -> Dict[str, Any]:
    """Create a sample fraud alert for testing."""
    return {
        "alert_id": str(uuid.uuid4()),
        "transaction_id": str(uuid.uuid4()),
        "customer_id_hash": "a1b2c3d4e5f67890",
        "alert_type": "VELOCITY_FRAUD",
        "severity": "HIGH",
        "risk_score": 0.85,
        "confidence_score": 0.92,
        "triggered_rules": ["high_velocity", "amount_anomaly"],
        "created_at": datetime.now(timezone.utc).isoformat()
    }


@pytest.fixture
def sample_customer() -> Dict[str, Any]:
    """Create a sample customer profile for testing."""
    return {
        "customer_id": "cust_test123",
        "avg_transaction_amount": 150.0,
        "preferred_categories": ["GROCERY", "RESTAURANT", "RETAIL"],
        "home_location": {
            "country": "US",
            "state": "CA",
            "city": "San Francisco",
            "zip_code": "94102",
            "lat": 37.7749,
            "lon": -122.4194
        },
        "spending_pattern": "moderate",
        "risk_profile": "low"
    }


@pytest.fixture
def sample_merchant() -> Dict[str, Any]:
    """Create a sample merchant profile for testing."""
    return {
        "merchant_id": "merch_test456",
        "name": "Test Store San Francisco",
        "category": "RETAIL",
        "category_code": "5999",
        "location": {
            "country": "US",
            "state": "CA",
            "city": "San Francisco",
            "zip_code": "94102",
            "lat": 37.7749,
            "lon": -122.4194
        },
        "risk_score": 0.25
    }


@pytest.fixture
def mock_kafka_producer():
    """Create a mock Kafka producer."""
    with patch('confluent_kafka.Producer') as mock:
        producer = MagicMock()
        mock.return_value = producer
        yield producer


@pytest.fixture
def mock_schema_registry():
    """Create a mock Schema Registry client."""
    with patch('confluent_kafka.schema_registry.SchemaRegistryClient') as mock:
        client = MagicMock()
        mock.return_value = client
        yield client


@pytest.fixture
def mock_snowflake_connection():
    """Create a mock Snowflake connection."""
    with patch('snowflake.connector.connect') as mock:
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        mock.return_value = conn
        yield conn


@pytest.fixture
def batch_transactions(sample_transaction) -> list:
    """Create a batch of transactions for testing."""
    transactions = []
    for i in range(10):
        txn = sample_transaction.copy()
        txn["transaction_id"] = str(uuid.uuid4())
        txn["amount"] = str(Decimal("50.00") + Decimal(str(i * 25)))
        transactions.append(txn)
    return transactions


@pytest.fixture
def mixed_validity_transactions(sample_transaction, sample_invalid_transaction) -> list:
    """Create a mix of valid and invalid transactions."""
    transactions = []
    for i in range(5):
        txn = sample_transaction.copy()
        txn["transaction_id"] = str(uuid.uuid4())
        transactions.append(txn)
    for i in range(3):
        txn = sample_invalid_transaction.copy()
        txn["transaction_id"] = f"invalid-{i}"
        transactions.append(txn)
    return transactions
