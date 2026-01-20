"""
Unit tests for transaction producer module.
"""
import pytest
from unittest.mock import patch, MagicMock
from decimal import Decimal


class TestCustomerDataclass:
    """Tests for Customer dataclass."""

    def test_customer_creation(self):
        """Test creating a Customer instance."""
        from src.data_generator.transaction_producer import Customer

        customer = Customer(
            customer_id="cust_123",
            avg_transaction_amount=150.0,
            preferred_categories=["GROCERY", "RESTAURANT"],
            home_location={"country": "US", "state": "CA", "city": "SF"},
            spending_pattern="moderate",
            risk_profile="low"
        )

        assert customer.customer_id == "cust_123"
        assert customer.avg_transaction_amount == 150.0
        assert len(customer.preferred_categories) == 2


class TestMerchantDataclass:
    """Tests for Merchant dataclass."""

    def test_merchant_creation(self):
        """Test creating a Merchant instance."""
        from src.data_generator.transaction_producer import Merchant

        merchant = Merchant(
            merchant_id="merch_456",
            name="Test Store",
            category="RETAIL",
            category_code="5999",
            location={"country": "US", "state": "CA"},
            risk_score=0.25
        )

        assert merchant.merchant_id == "merch_456"
        assert merchant.name == "Test Store"
        assert merchant.risk_score == 0.25


class TestTransactionProducerInit:
    """Tests for TransactionProducer initialization."""

    @patch('src.data_generator.transaction_producer.Producer')
    @patch('src.data_generator.transaction_producer.SchemaRegistryClient')
    @patch('src.data_generator.transaction_producer.AvroSerializer')
    @patch('builtins.open', create=True)
    def test_initialization(self, mock_open, mock_serializer, mock_sr, mock_producer):
        """Test TransactionProducer initialization."""
        mock_open.return_value.__enter__.return_value.read.return_value = "{}"

        from src.data_generator.transaction_producer import TransactionProducer

        producer = TransactionProducer()

        assert producer.customers is not None
        assert len(producer.customers) == 1000
        assert producer.merchants is not None
        assert len(producer.merchants) == 500

    @patch('src.data_generator.transaction_producer.Producer')
    @patch('src.data_generator.transaction_producer.SchemaRegistryClient')
    @patch('src.data_generator.transaction_producer.AvroSerializer')
    @patch('builtins.open', create=True)
    def test_generates_customers(self, mock_open, mock_serializer, mock_sr, mock_producer):
        """Test that producer generates customer profiles."""
        mock_open.return_value.__enter__.return_value.read.return_value = "{}"

        from src.data_generator.transaction_producer import TransactionProducer

        producer = TransactionProducer()

        for customer in producer.customers[:10]:
            assert customer.customer_id.startswith("cust_")
            assert customer.avg_transaction_amount > 0
            assert customer.spending_pattern in ["conservative", "moderate", "high_spender"]

    @patch('src.data_generator.transaction_producer.Producer')
    @patch('src.data_generator.transaction_producer.SchemaRegistryClient')
    @patch('src.data_generator.transaction_producer.AvroSerializer')
    @patch('builtins.open', create=True)
    def test_generates_merchants(self, mock_open, mock_serializer, mock_sr, mock_producer):
        """Test that producer generates merchant profiles."""
        mock_open.return_value.__enter__.return_value.read.return_value = "{}"

        from src.data_generator.transaction_producer import TransactionProducer

        producer = TransactionProducer()

        for merchant in producer.merchants[:10]:
            assert merchant.merchant_id.startswith("merch_")
            assert merchant.category in ["GROCERY", "GAS_STATION", "RESTAURANT",
                                         "RETAIL", "ONLINE", "ATM", "HOTEL",
                                         "AIRLINE", "PHARMACY", "AUTOMOTIVE",
                                         "ENTERTAINMENT", "HEALTHCARE", "EDUCATION",
                                         "UTILITIES", "OTHER"]


class TestTransactionGeneration:
    """Tests for transaction generation."""

    @patch('src.data_generator.transaction_producer.Producer')
    @patch('src.data_generator.transaction_producer.SchemaRegistryClient')
    @patch('src.data_generator.transaction_producer.AvroSerializer')
    @patch('builtins.open', create=True)
    def test_generate_transaction(self, mock_open, mock_serializer, mock_sr, mock_producer):
        """Test generating a single transaction."""
        mock_open.return_value.__enter__.return_value.read.return_value = "{}"

        from src.data_generator.transaction_producer import TransactionProducer

        producer = TransactionProducer()
        txn = producer._generate_transaction()

        assert "transaction_id" in txn
        assert "timestamp" in txn
        assert "customer_id" in txn
        assert "amount" in txn
        assert "merchant_id" in txn
        assert "status" in txn

    @patch('src.data_generator.transaction_producer.Producer')
    @patch('src.data_generator.transaction_producer.SchemaRegistryClient')
    @patch('src.data_generator.transaction_producer.AvroSerializer')
    @patch('builtins.open', create=True)
    def test_transaction_has_valid_amount(self, mock_open, mock_serializer, mock_sr, mock_producer):
        """Test that generated transaction has valid amount."""
        mock_open.return_value.__enter__.return_value.read.return_value = "{}"

        from src.data_generator.transaction_producer import TransactionProducer

        producer = TransactionProducer()

        for _ in range(10):
            txn = producer._generate_transaction()
            amount = float(txn["amount"])
            assert amount >= 1.0  # Minimum amount

    @patch('src.data_generator.transaction_producer.Producer')
    @patch('src.data_generator.transaction_producer.SchemaRegistryClient')
    @patch('src.data_generator.transaction_producer.AvroSerializer')
    @patch('builtins.open', create=True)
    def test_transaction_has_valid_status(self, mock_open, mock_serializer, mock_sr, mock_producer):
        """Test that generated transaction has valid status."""
        mock_open.return_value.__enter__.return_value.read.return_value = "{}"

        from src.data_generator.transaction_producer import TransactionProducer

        producer = TransactionProducer()

        for _ in range(10):
            txn = producer._generate_transaction()
            assert txn["status"] in ["APPROVED", "DECLINED", "PENDING"]


class TestTransactionAmountGeneration:
    """Tests for transaction amount generation."""

    @patch('src.data_generator.transaction_producer.Producer')
    @patch('src.data_generator.transaction_producer.SchemaRegistryClient')
    @patch('src.data_generator.transaction_producer.AvroSerializer')
    @patch('builtins.open', create=True)
    def test_amount_based_on_customer_profile(self, mock_open, mock_serializer, mock_sr, mock_producer):
        """Test that amount is influenced by customer profile."""
        mock_open.return_value.__enter__.return_value.read.return_value = "{}"

        from src.data_generator.transaction_producer import TransactionProducer, Customer, Merchant

        producer = TransactionProducer()

        conservative_customer = Customer(
            customer_id="cust_1",
            avg_transaction_amount=50.0,
            preferred_categories=["GROCERY"],
            home_location={"country": "US"},
            spending_pattern="conservative",
            risk_profile="low"
        )

        high_spender = Customer(
            customer_id="cust_2",
            avg_transaction_amount=500.0,
            preferred_categories=["RETAIL"],
            home_location={"country": "US"},
            spending_pattern="high_spender",
            risk_profile="low"
        )

        merchant = producer.merchants[0]

        conservative_amounts = [
            float(producer._generate_transaction_amount(conservative_customer, merchant))
            for _ in range(20)
        ]
        high_spender_amounts = [
            float(producer._generate_transaction_amount(high_spender, merchant))
            for _ in range(20)
        ]

        avg_conservative = sum(conservative_amounts) / len(conservative_amounts)
        avg_high_spender = sum(high_spender_amounts) / len(high_spender_amounts)

        # High spender should generally have higher amounts
        assert avg_high_spender > avg_conservative


class TestFraudInjection:
    """Tests for fraud injection in producer."""

    @patch('src.data_generator.transaction_producer.Producer')
    @patch('src.data_generator.transaction_producer.SchemaRegistryClient')
    @patch('src.data_generator.transaction_producer.AvroSerializer')
    @patch('builtins.open', create=True)
    def test_fraud_injection_rate(self, mock_open, mock_serializer, mock_sr, mock_producer):
        """Test fraud injection respects rate setting."""
        mock_open.return_value.__enter__.return_value.read.return_value = "{}"

        from src.data_generator.transaction_producer import TransactionProducer

        producer = TransactionProducer()

        # Generate many transactions and check fraud rate
        fraud_count = 0
        total_count = 1000

        for _ in range(total_count):
            result = producer._should_inject_fraud()
            if result:
                fraud_count += 1

        # Expected rate is ~5% (default)
        fraud_rate = fraud_count / total_count
        # Allow for statistical variance
        assert 0.02 <= fraud_rate <= 0.10


class TestProducerStats:
    """Tests for producer statistics tracking."""

    @patch('src.data_generator.transaction_producer.Producer')
    @patch('src.data_generator.transaction_producer.SchemaRegistryClient')
    @patch('src.data_generator.transaction_producer.AvroSerializer')
    @patch('builtins.open', create=True)
    def test_initial_stats(self, mock_open, mock_serializer, mock_sr, mock_producer):
        """Test initial statistics are zero."""
        mock_open.return_value.__enter__.return_value.read.return_value = "{}"

        from src.data_generator.transaction_producer import TransactionProducer

        producer = TransactionProducer()

        assert producer.stats["transactions_sent"] == 0
        assert producer.stats["fraud_transactions"] == 0
        assert producer.stats["errors"] == 0

    @patch('src.data_generator.transaction_producer.Producer')
    @patch('src.data_generator.transaction_producer.SchemaRegistryClient')
    @patch('src.data_generator.transaction_producer.AvroSerializer')
    @patch('builtins.open', create=True)
    def test_stop_sets_running_false(self, mock_open, mock_serializer, mock_sr, mock_producer):
        """Test that stop() sets running to False."""
        mock_open.return_value.__enter__.return_value.read.return_value = "{}"
        mock_producer.return_value.flush = MagicMock()

        from src.data_generator.transaction_producer import TransactionProducer

        producer = TransactionProducer()
        producer.running = True
        producer.stop()

        assert producer.running is False
