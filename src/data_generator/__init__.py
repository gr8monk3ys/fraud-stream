"""Data generation package for realistic financial transaction simulation."""

from .transaction_producer import TransactionProducer, Customer, Merchant
from .fraud_injector import FraudInjector

__all__ = [
    # Transaction producer
    "TransactionProducer",
    "Customer",
    "Merchant",
    # Fraud injection
    "FraudInjector",
]
