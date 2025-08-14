"""
Financial Transaction Producer for generating realistic transaction data.
Simulates credit card transactions with various patterns and fraud scenarios.
"""
import json
import random
import time
import uuid
import threading
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Tuple
import logging
from dataclasses import dataclass, asdict

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

from src.config import settings


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class Customer:
    """Customer profile for transaction generation."""
    customer_id: str
    avg_transaction_amount: float
    preferred_categories: List[str]
    home_location: Dict[str, str]
    spending_pattern: str  # conservative, moderate, high_spender
    risk_profile: str  # low, medium, high


@dataclass
class Merchant:
    """Merchant profile for transaction generation."""
    merchant_id: str
    name: str
    category: str
    category_code: str
    location: Dict[str, str]
    risk_score: float


class TransactionProducer:
    """Produces realistic financial transactions to Kafka."""
    
    def __init__(self):
        """Initialize the transaction producer."""
        self.settings = settings
        self.producer = self._create_kafka_producer()
        self.schema_registry_client = self._create_schema_registry_client()
        self.avro_serializer = self._create_avro_serializer()
        
        # Generate sample data
        self.customers = self._generate_customers(1000)
        self.merchants = self._generate_merchants(500)
        
        # Transaction patterns
        self.merchant_categories = [
            ("GROCERY", "5411"), ("GAS_STATION", "5541"), ("RESTAURANT", "5812"),
            ("RETAIL", "5999"), ("ONLINE", "5734"), ("ATM", "6011"),
            ("HOTEL", "7011"), ("AIRLINE", "4511"), ("PHARMACY", "5912"),
            ("AUTOMOTIVE", "5531"), ("ENTERTAINMENT", "7832"), ("HEALTHCARE", "8011"),
            ("EDUCATION", "8220"), ("UTILITIES", "4900"), ("OTHER", "9999")
        ]
        
        self.us_locations = [
            {"country": "US", "state": "CA", "city": "San Francisco", "zip_code": "94102", "lat": 37.7749, "lon": -122.4194},
            {"country": "US", "state": "NY", "city": "New York", "zip_code": "10001", "lat": 40.7128, "lon": -74.0060},
            {"country": "US", "state": "TX", "city": "Austin", "zip_code": "73301", "lat": 30.2672, "lon": -97.7431},
            {"country": "US", "state": "WA", "city": "Seattle", "zip_code": "98101", "lat": 47.6062, "lon": -122.3321},
            {"country": "US", "state": "FL", "city": "Miami", "zip_code": "33101", "lat": 25.7617, "lon": -80.1918},
            {"country": "US", "state": "IL", "city": "Chicago", "zip_code": "60601", "lat": 41.8781, "lon": -87.6298},
            {"country": "US", "state": "MA", "city": "Boston", "zip_code": "02101", "lat": 42.3601, "lon": -71.0589},
            {"country": "US", "state": "CO", "city": "Denver", "zip_code": "80201", "lat": 39.7392, "lon": -104.9903},
        ]
        
        self.running = False
        self.stats = {
            "transactions_sent": 0,
            "fraud_transactions": 0,
            "errors": 0,
            "start_time": None
        }
    
    def _create_kafka_producer(self) -> Producer:
        """Create Kafka producer instance."""
        config = self.settings.get_kafka_config()
        config.update({
            'client.id': 'transaction-producer',
            'acks': 'all',
            'retries': 3,
            'retry.backoff.ms': 100,
            'linger.ms': 5,
            'batch.size': 16384,
            'compression.type': 'snappy',
        })
        return Producer(config)
    
    def _create_schema_registry_client(self) -> SchemaRegistryClient:
        """Create Schema Registry client."""
        config = {'url': self.settings.kafka.schema_registry_url}
        
        if self.settings.kafka.schema_registry_username:
            config.update({
                'basic.auth.user.info': f"{self.settings.kafka.schema_registry_username}:{self.settings.kafka.schema_registry_password}"
            })
        
        return SchemaRegistryClient(config)
    
    def _create_avro_serializer(self) -> AvroSerializer:
        """Create Avro serializer for transactions."""
        # Load schema from file
        with open('src/schemas/transaction.avsc', 'r') as f:
            schema_str = f.read()
        
        return AvroSerializer(
            schema_registry_client=self.schema_registry_client,
            schema_str=schema_str,
            to_dict=self._transaction_to_dict
        )
    
    def _transaction_to_dict(self, transaction: Dict, ctx: SerializationContext) -> Dict:
        """Convert transaction object to dictionary for serialization."""
        return transaction
    
    def _generate_customers(self, count: int) -> List[Customer]:
        """Generate customer profiles for transaction simulation."""
        customers = []
        spending_patterns = ["conservative", "moderate", "high_spender"]
        risk_profiles = ["low", "medium", "high"]
        
        for i in range(count):
            location = random.choice(self.us_locations)
            spending_pattern = random.choices(
                spending_patterns, 
                weights=[0.4, 0.5, 0.1]  # Most customers are conservative/moderate
            )[0]
            
            # Set average transaction amount based on spending pattern
            if spending_pattern == "conservative":
                avg_amount = random.uniform(25, 100)
            elif spending_pattern == "moderate":
                avg_amount = random.uniform(75, 300)
            else:  # high_spender
                avg_amount = random.uniform(200, 1000)
            
            customer = Customer(
                customer_id=f"cust_{str(uuid.uuid4())[:8]}",
                avg_transaction_amount=avg_amount,
                preferred_categories=random.choices(
                    [cat[0] for cat in self.merchant_categories], 
                    k=random.randint(3, 6)
                ),
                home_location=location,
                spending_pattern=spending_pattern,
                risk_profile=random.choices(
                    risk_profiles,
                    weights=[0.7, 0.25, 0.05]  # Most customers are low risk
                )[0]
            )
            customers.append(customer)
        
        return customers
    
    def _generate_merchants(self, count: int) -> List[Merchant]:
        """Generate merchant profiles."""
        merchants = []
        
        for i in range(count):
            category, mcc = random.choice(self.merchant_categories)
            location = random.choice(self.us_locations)
            
            # Generate merchant name based on category
            category_names = {
                "GROCERY": ["Fresh Market", "SuperMart", "Corner Store", "Organic Foods"],
                "GAS_STATION": ["Shell", "Chevron", "BP", "Exxon", "Arco"],
                "RESTAURANT": ["Bistro", "Cafe", "Grill", "Kitchen", "Diner"],
                "RETAIL": ["Fashion Store", "Electronics Plus", "Department Store"],
                "ONLINE": ["E-Commerce", "Online Store", "Digital Marketplace"],
                "ATM": ["Bank ATM", "Credit Union ATM"],
                "HOTEL": ["Grand Hotel", "Business Inn", "Resort"],
                "AIRLINE": ["Airways", "Airlines", "Air Travel"],
                "PHARMACY": ["Health Pharmacy", "Quick Meds", "Care Drugs"],
                "AUTOMOTIVE": ["Auto Parts", "Car Service", "Oil Change"],
                "ENTERTAINMENT": ["Cinema", "Theater", "Gaming"],
                "HEALTHCARE": ["Medical Center", "Clinic", "Hospital"],
                "EDUCATION": ["University", "College", "School"],
                "UTILITIES": ["Electric Co", "Gas Co", "Water Dept"],
                "OTHER": ["Service Provider", "General Store"]
            }
            
            base_name = random.choice(category_names.get(category, ["Business"]))
            name = f"{base_name} {location['city']}"
            
            # Risk score based on category and random factors
            category_risk = {
                "GROCERY": 0.1, "GAS_STATION": 0.15, "RESTAURANT": 0.2,
                "RETAIL": 0.25, "ONLINE": 0.4, "ATM": 0.1,
                "HOTEL": 0.3, "AIRLINE": 0.2, "PHARMACY": 0.15,
                "AUTOMOTIVE": 0.25, "ENTERTAINMENT": 0.3, "HEALTHCARE": 0.1,
                "EDUCATION": 0.1, "UTILITIES": 0.05, "OTHER": 0.35
            }
            
            base_risk = category_risk.get(category, 0.25)
            risk_score = min(1.0, base_risk + random.uniform(-0.1, 0.2))
            
            merchant = Merchant(
                merchant_id=f"merch_{str(uuid.uuid4())[:8]}",
                name=name,
                category=category,
                category_code=mcc,
                location=location,
                risk_score=risk_score
            )
            merchants.append(merchant)
        
        return merchants
    
    def _generate_transaction_amount(self, customer: Customer, merchant: Merchant) -> Decimal:
        """Generate realistic transaction amount based on customer and merchant."""
        base_amount = customer.avg_transaction_amount
        
        # Category-based adjustments
        category_multipliers = {
            "GROCERY": (0.3, 1.5), "GAS_STATION": (0.5, 1.2), "RESTAURANT": (0.4, 2.0),
            "RETAIL": (0.2, 3.0), "ONLINE": (0.3, 2.5), "ATM": (0.5, 1.0),
            "HOTEL": (2.0, 10.0), "AIRLINE": (1.5, 8.0), "PHARMACY": (0.1, 0.8),
            "AUTOMOTIVE": (0.5, 3.0), "ENTERTAINMENT": (0.3, 2.0), "HEALTHCARE": (0.5, 5.0),
            "EDUCATION": (0.5, 5.0), "UTILITIES": (0.8, 2.0), "OTHER": (0.2, 2.0)
        }
        
        min_mult, max_mult = category_multipliers.get(merchant.category, (0.5, 2.0))
        multiplier = random.uniform(min_mult, max_mult)
        
        # Add some randomness
        amount = base_amount * multiplier * random.uniform(0.7, 1.3)
        
        # Round to 2 decimal places and ensure minimum
        amount = max(1.0, round(amount, 2))
        
        return Decimal(str(amount))
    
    def _should_inject_fraud(self) -> bool:
        """Determine if this transaction should be fraudulent."""
        return random.random() < self.settings.transaction_generator.fraud_injection_rate
    
    def _inject_fraud_patterns(self, transaction: Dict, customer: Customer) -> Dict:
        """Inject various fraud patterns into the transaction."""
        fraud_patterns = [
            "velocity_fraud",      # Many transactions in short time
            "amount_fraud",        # Unusually high amount
            "geographic_fraud",    # Transaction far from home
            "time_fraud",          # Transaction at unusual time
            "merchant_fraud"       # High-risk merchant
        ]
        
        pattern = random.choice(fraud_patterns)
        
        if pattern == "velocity_fraud":
            # This will be handled by generating multiple transactions
            pass
        elif pattern == "amount_fraud":
            # 5-20x normal amount
            normal_amount = float(transaction["amount"])
            fraud_multiplier = random.uniform(5, 20)
            transaction["amount"] = str(Decimal(str(normal_amount * fraud_multiplier)))
        elif pattern == "geographic_fraud":
            # Transaction in different country or far location
            international_locations = [
                {"country": "MX", "state": None, "city": "Mexico City", "zip_code": None, "lat": 19.4326, "lon": -99.1332},
                {"country": "CA", "state": "ON", "city": "Toronto", "zip_code": None, "lat": 43.6532, "lon": -79.3832},
                {"country": "GB", "state": None, "city": "London", "zip_code": None, "lat": 51.5074, "lon": -0.1278},
            ]
            fraud_location = random.choice(international_locations)
            transaction["location"] = fraud_location
            transaction["is_international"] = fraud_location["country"] != "US"
        elif pattern == "time_fraud":
            # Transaction at 2-4 AM
            fraud_hour = random.randint(2, 4)
            current_time = datetime.fromtimestamp(transaction["timestamp"] / 1000)
            fraud_time = current_time.replace(hour=fraud_hour, minute=random.randint(0, 59))
            transaction["timestamp"] = int(fraud_time.timestamp() * 1000)
        elif pattern == "merchant_fraud":
            # Select high-risk merchant
            high_risk_merchants = [m for m in self.merchants if m.risk_score > 0.7]
            if high_risk_merchants:
                fraud_merchant = random.choice(high_risk_merchants)
                transaction["merchant_id"] = fraud_merchant.merchant_id
                transaction["merchant_name"] = fraud_merchant.name
                transaction["merchant_category"] = fraud_merchant.category
                transaction["merchant_category_code"] = fraud_merchant.category_code
        
        # Add fraud indicators to metadata
        if "metadata" not in transaction:
            transaction["metadata"] = {}
        transaction["metadata"]["fraud_pattern"] = pattern
        transaction["metadata"]["is_synthetic_fraud"] = "true"
        
        return transaction
    
    def _generate_transaction(self) -> Dict:
        """Generate a single financial transaction."""
        customer = random.choice(self.customers)
        merchant = random.choice(self.merchants)
        
        # Base transaction timestamp
        now = datetime.now()
        
        # Simulate late arrivals occasionally
        if (self.settings.transaction_generator.simulate_late_arrivals and 
            random.random() < self.settings.transaction_generator.late_arrival_probability):
            # Make transaction 1-24 hours old
            delay_hours = random.uniform(1, 24)
            transaction_time = now - timedelta(hours=delay_hours)
        else:
            # Recent transaction (last 5 minutes)
            delay_seconds = random.uniform(0, 300)
            transaction_time = now - timedelta(seconds=delay_seconds)
        
        # Generate transaction
        transaction = {
            "transaction_id": str(uuid.uuid4()),
            "timestamp": int(transaction_time.timestamp() * 1000),
            "customer_id": customer.customer_id,
            "card_number_last4": f"{random.randint(1000, 9999)}",
            "card_type": random.choice(["CREDIT", "DEBIT", "PREPAID"]),
            "amount": str(self._generate_transaction_amount(customer, merchant)),
            "currency": "USD",
            "merchant_id": merchant.merchant_id,
            "merchant_name": merchant.name,
            "merchant_category_code": merchant.category_code,
            "merchant_category": merchant.category,
            "location": merchant.location,
            "channel": random.choices(
                ["ONLINE", "POS", "ATM", "MOBILE", "PHONE"],
                weights=[0.4, 0.35, 0.1, 0.1, 0.05]
            )[0],
            "status": random.choices(
                ["APPROVED", "DECLINED", "PENDING"],
                weights=[0.85, 0.10, 0.05]
            )[0],
            "risk_score": None,  # Will be calculated by streaming pipeline
            "authorization_code": f"AUTH{random.randint(100000, 999999)}",
            "processor_response_code": "00" if random.random() < 0.9 else random.choice(["05", "14", "51", "61"]),
            "is_international": merchant.location["country"] != customer.home_location["country"],
            "customer_present": random.choice([True, False]) if merchant.category != "ONLINE" else False,
            "entry_method": random.choice(["CHIP", "SWIPE", "CONTACTLESS", "MANUAL", "ONLINE"]),
            "metadata": {
                "customer_spending_pattern": customer.spending_pattern,
                "merchant_risk_score": str(merchant.risk_score),
                "generated_at": now.isoformat()
            },
            "created_at": int(now.timestamp() * 1000),
            "version": "1.0"
        }
        
        # Inject fraud patterns if applicable
        if self._should_inject_fraud():
            transaction = self._inject_fraud_patterns(transaction, customer)
            self.stats["fraud_transactions"] += 1
        
        return transaction
    
    def _delivery_callback(self, err, msg):
        """Callback for message delivery confirmation."""
        if err is not None:
            logger.error(f"Failed to deliver message: {err}")
            self.stats["errors"] += 1
        else:
            self.stats["transactions_sent"] += 1
            if self.stats["transactions_sent"] % 100 == 0:
                self._log_stats()
    
    def _log_stats(self):
        """Log current statistics."""
        if self.stats["start_time"]:
            elapsed = time.time() - self.stats["start_time"]
            rate = self.stats["transactions_sent"] / elapsed if elapsed > 0 else 0
            
            logger.info(
                f"Stats - Sent: {self.stats['transactions_sent']}, "
                f"Fraud: {self.stats['fraud_transactions']}, "
                f"Errors: {self.stats['errors']}, "
                f"Rate: {rate:.1f} TPS"
            )
    
    def produce_transactions(self, duration_seconds: Optional[int] = None):
        """Produce transactions continuously."""
        self.running = True
        self.stats["start_time"] = time.time()
        
        logger.info(f"Starting transaction producer - Rate: {self.settings.transaction_generator.transaction_rate_per_second} TPS")
        
        target_interval = 1.0 / self.settings.transaction_generator.transaction_rate_per_second
        
        try:
            start_time = time.time()
            
            while self.running:
                if duration_seconds and (time.time() - start_time) > duration_seconds:
                    break
                
                # Generate and send transaction
                transaction = self._generate_transaction()
                
                self.producer.produce(
                    topic=self.settings.kafka.transactions_topic,
                    key=transaction["customer_id"],
                    value=self.avro_serializer(
                        transaction, 
                        SerializationContext(
                            self.settings.kafka.transactions_topic, 
                            MessageField.VALUE
                        )
                    ),
                    callback=self._delivery_callback
                )
                
                # Poll for delivery confirmations
                self.producer.poll(0)
                
                # Rate limiting
                time.sleep(target_interval)
                
        except KeyboardInterrupt:
            logger.info("Received interrupt signal, stopping producer...")
        except Exception as e:
            logger.error(f"Error in transaction producer: {e}")
            raise
        finally:
            self.stop()
    
    def stop(self):
        """Stop the producer gracefully."""
        self.running = False
        logger.info("Stopping transaction producer...")
        
        # Flush any remaining messages
        self.producer.flush(timeout=10)
        
        # Final stats
        self._log_stats()
        logger.info("Transaction producer stopped.")


def main():
    """Main function to run the transaction producer."""
    producer = TransactionProducer()
    
    try:
        # Run for 1 hour by default, or indefinitely if not specified
        producer.produce_transactions(duration_seconds=3600)
    except KeyboardInterrupt:
        logger.info("Shutting down transaction producer...")
    except Exception as e:
        logger.error(f"Transaction producer error: {e}")
        raise


if __name__ == "__main__":
    main()
