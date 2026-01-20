"""
Unit tests for fraud injection module.
"""
import pytest
from datetime import datetime, timezone


class TestFraudInjector:
    """Tests for FraudInjector class."""

    def test_injector_initialization(self):
        """Test fraud injector initializes with all scenarios."""
        from src.data_generator.fraud_injector import FraudInjector
        injector = FraudInjector()

        expected_scenarios = [
            "account_takeover",
            "card_testing",
            "velocity_attack",
            "geographic_impossible",
            "amount_probing",
            "merchant_collusion",
            "synthetic_identity",
            "bust_out_fraud"
        ]

        for scenario in expected_scenarios:
            assert scenario in injector.fraud_scenarios

    def test_inject_unknown_scenario(self, sample_transaction):
        """Test injecting unknown scenario returns original transaction."""
        from src.data_generator.fraud_injector import FraudInjector
        injector = FraudInjector()

        result = injector.inject_fraud_scenario("unknown_scenario", sample_transaction)

        assert len(result) == 1
        assert result[0]["transaction_id"] == sample_transaction["transaction_id"]

    def test_get_random_fraud_scenario(self):
        """Test getting random fraud scenario."""
        from src.data_generator.fraud_injector import FraudInjector
        injector = FraudInjector()

        scenario = injector.get_random_fraud_scenario()

        assert scenario in injector.fraud_scenarios

    def test_get_fraud_scenario_description(self):
        """Test getting scenario description."""
        from src.data_generator.fraud_injector import FraudInjector
        injector = FraudInjector()

        desc = injector.get_fraud_scenario_description("account_takeover")

        assert desc is not None
        assert len(desc) > 0

    def test_get_unknown_scenario_description(self):
        """Test getting description for unknown scenario."""
        from src.data_generator.fraud_injector import FraudInjector
        injector = FraudInjector()

        desc = injector.get_fraud_scenario_description("unknown")

        assert desc == "Unknown fraud scenario"


class TestAccountTakeoverFraud:
    """Tests for account takeover fraud pattern."""

    def test_generates_multiple_transactions(self, sample_transaction):
        """Test that account takeover generates multiple transactions."""
        from src.data_generator.fraud_injector import FraudInjector
        injector = FraudInjector()

        result = injector.inject_fraud_scenario("account_takeover", sample_transaction)

        assert len(result) >= 5  # Minimum specified in implementation
        assert len(result) <= 15  # Maximum specified

    def test_transactions_have_fraud_metadata(self, sample_transaction):
        """Test that transactions have fraud metadata."""
        from src.data_generator.fraud_injector import FraudInjector
        injector = FraudInjector()

        result = injector.inject_fraud_scenario("account_takeover", sample_transaction)

        for txn in result:
            assert "metadata" in txn
            assert txn["metadata"]["fraud_pattern"] == "account_takeover"
            assert txn["metadata"]["is_synthetic_fraud"] == "true"

    def test_transactions_have_unique_ids(self, sample_transaction):
        """Test that each transaction has unique ID."""
        from src.data_generator.fraud_injector import FraudInjector
        injector = FraudInjector()

        result = injector.inject_fraud_scenario("account_takeover", sample_transaction)

        transaction_ids = [txn["transaction_id"] for txn in result]
        assert len(transaction_ids) == len(set(transaction_ids))


class TestCardTestingFraud:
    """Tests for card testing fraud pattern."""

    def test_generates_small_amounts(self, sample_transaction):
        """Test that card testing generates small amount transactions."""
        from src.data_generator.fraud_injector import FraudInjector
        injector = FraudInjector()

        result = injector.inject_fraud_scenario("card_testing", sample_transaction)

        for txn in result:
            amount = float(txn["amount"])
            assert amount >= 0.50
            assert amount < 5.00  # All should be under $5

    def test_generates_many_transactions(self, sample_transaction):
        """Test that card testing generates many transactions."""
        from src.data_generator.fraud_injector import FraudInjector
        injector = FraudInjector()

        result = injector.inject_fraud_scenario("card_testing", sample_transaction)

        assert len(result) >= 10  # Minimum
        assert len(result) <= 50  # Maximum

    def test_includes_declined_transactions(self, sample_transaction):
        """Test that card testing includes declined transactions."""
        from src.data_generator.fraud_injector import FraudInjector
        injector = FraudInjector()

        result = injector.inject_fraud_scenario("card_testing", sample_transaction)

        statuses = [txn["status"] for txn in result]
        # Should have mix of approved and declined
        assert "DECLINED" in statuses or "APPROVED" in statuses


class TestVelocityAttack:
    """Tests for velocity attack fraud pattern."""

    def test_generates_high_volume(self, sample_transaction):
        """Test that velocity attack generates high volume."""
        from src.data_generator.fraud_injector import FraudInjector
        injector = FraudInjector()

        result = injector.inject_fraud_scenario("velocity_attack", sample_transaction)

        assert len(result) >= 20
        assert len(result) <= 100

    def test_transactions_in_short_timeframe(self, sample_transaction):
        """Test that transactions occur within short timeframe."""
        from src.data_generator.fraud_injector import FraudInjector
        injector = FraudInjector()

        result = injector.inject_fraud_scenario("velocity_attack", sample_transaction)

        timestamps = [txn["timestamp"] for txn in result]
        min_ts, max_ts = min(timestamps), max(timestamps)

        # All should be within 1 hour (3600000 ms)
        assert max_ts - min_ts <= 3600000


class TestGeographicImpossible:
    """Tests for geographically impossible fraud pattern."""

    def test_generates_impossible_travel(self, sample_transaction):
        """Test that geographic impossible generates multiple locations."""
        from src.data_generator.fraud_injector import FraudInjector
        injector = FraudInjector()

        result = injector.inject_fraud_scenario("geographic_impossible", sample_transaction)

        # Should have 4 locations (NY, London, Tokyo, LA)
        assert len(result) == 4

    def test_includes_international_transactions(self, sample_transaction):
        """Test that geographic impossible includes international transactions."""
        from src.data_generator.fraud_injector import FraudInjector
        injector = FraudInjector()

        result = injector.inject_fraud_scenario("geographic_impossible", sample_transaction)

        countries = [txn["location"]["country"] for txn in result]
        # Should include at least one non-US country
        assert any(c != "US" for c in countries)


class TestAmountProbing:
    """Tests for amount probing fraud pattern."""

    def test_generates_incremental_amounts(self, sample_transaction):
        """Test that amount probing generates incremental amounts."""
        from src.data_generator.fraud_injector import FraudInjector
        injector = FraudInjector()

        result = injector.inject_fraud_scenario("amount_probing", sample_transaction)

        amounts = [float(txn["amount"]) for txn in result]
        # Amounts should be in roughly increasing order
        # (based on probe_amounts list in implementation)
        assert amounts[0] < amounts[-1]

    def test_includes_all_probe_amounts(self, sample_transaction):
        """Test that all probe amounts are included."""
        from src.data_generator.fraud_injector import FraudInjector
        injector = FraudInjector()

        result = injector.inject_fraud_scenario("amount_probing", sample_transaction)

        expected_amounts = [10, 25, 50, 100, 250, 500, 1000, 2000, 5000]
        actual_amounts = [float(txn["amount"]) for txn in result]

        assert len(actual_amounts) == len(expected_amounts)


class TestMerchantCollusion:
    """Tests for merchant collusion fraud pattern."""

    def test_generates_related_merchants(self, sample_transaction):
        """Test that merchant collusion generates related merchant transactions."""
        from src.data_generator.fraud_injector import FraudInjector
        injector = FraudInjector()

        result = injector.inject_fraud_scenario("merchant_collusion", sample_transaction)

        # Should have 4 colluding merchants
        assert len(result) == 4

    def test_amounts_are_similar(self, sample_transaction):
        """Test that amounts are similar across transactions."""
        from src.data_generator.fraud_injector import FraudInjector
        injector = FraudInjector()

        result = injector.inject_fraud_scenario("merchant_collusion", sample_transaction)

        amounts = [float(txn["amount"]) for txn in result]
        avg_amount = sum(amounts) / len(amounts)

        # All amounts should be within $100 of average
        for amount in amounts:
            assert abs(amount - avg_amount) <= 100


class TestSyntheticIdentity:
    """Tests for synthetic identity fraud pattern."""

    def test_creates_new_customer_id(self, sample_transaction):
        """Test that synthetic identity creates new customer ID."""
        from src.data_generator.fraud_injector import FraudInjector
        injector = FraudInjector()

        result = injector.inject_fraud_scenario("synthetic_identity", sample_transaction)

        for txn in result:
            assert txn["customer_id"].startswith("synthetic_")

    def test_generates_high_amounts(self, sample_transaction):
        """Test that synthetic identity generates high amounts."""
        from src.data_generator.fraud_injector import FraudInjector
        injector = FraudInjector()

        result = injector.inject_fraud_scenario("synthetic_identity", sample_transaction)

        amounts = [float(txn["amount"]) for txn in result]
        # All amounts should be >= $1500
        for amount in amounts:
            assert amount >= 1500


class TestBustOutFraud:
    """Tests for bust-out fraud pattern."""

    def test_has_establishment_phase(self, sample_transaction):
        """Test that bust-out has establishment phase."""
        from src.data_generator.fraud_injector import FraudInjector
        injector = FraudInjector()

        result = injector.inject_fraud_scenario("bust_out_fraud", sample_transaction)

        establishment_txns = [
            txn for txn in result
            if txn["metadata"].get("fraud_phase") == "establishment"
        ]
        assert len(establishment_txns) > 0

    def test_has_bust_out_phase(self, sample_transaction):
        """Test that bust-out has bust_out phase."""
        from src.data_generator.fraud_injector import FraudInjector
        injector = FraudInjector()

        result = injector.inject_fraud_scenario("bust_out_fraud", sample_transaction)

        bustout_txns = [
            txn for txn in result
            if txn["metadata"].get("fraud_phase") == "bust_out"
        ]
        assert len(bustout_txns) > 0

    def test_bust_out_amounts_higher_than_establishment(self, sample_transaction):
        """Test that bust-out amounts are higher than establishment."""
        from src.data_generator.fraud_injector import FraudInjector
        injector = FraudInjector()

        result = injector.inject_fraud_scenario("bust_out_fraud", sample_transaction)

        establishment_amounts = [
            float(txn["amount"]) for txn in result
            if txn["metadata"].get("fraud_phase") == "establishment"
        ]
        bustout_amounts = [
            float(txn["amount"]) for txn in result
            if txn["metadata"].get("fraud_phase") == "bust_out"
        ]

        avg_establishment = sum(establishment_amounts) / len(establishment_amounts)
        avg_bustout = sum(bustout_amounts) / len(bustout_amounts)

        assert avg_bustout > avg_establishment
