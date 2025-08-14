"""
Fraud Injection Module for creating sophisticated fraud patterns.
Generates coordinated fraud scenarios for testing fraud detection systems.
"""
import random
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import uuid
import threading
import logging

from src.config import settings

logger = logging.getLogger(__name__)


class FraudInjector:
    """
    Advanced fraud pattern injection for testing fraud detection systems.
    Creates realistic fraud scenarios including coordinated attacks.
    """
    
    def __init__(self):
        """Initialize the fraud injector."""
        self.settings = settings
        self.fraud_scenarios = {
            "account_takeover": self._generate_account_takeover,
            "card_testing": self._generate_card_testing,
            "velocity_attack": self._generate_velocity_attack,
            "geographic_impossible": self._generate_geographic_impossible,
            "amount_probing": self._generate_amount_probing,
            "merchant_collusion": self._generate_merchant_collusion,
            "synthetic_identity": self._generate_synthetic_identity,
            "bust_out_fraud": self._generate_bust_out_fraud,
        }
    
    def inject_fraud_scenario(self, scenario_name: str, base_transaction: Dict) -> List[Dict]:
        """
        Inject a specific fraud scenario into transactions.
        
        Args:
            scenario_name: Name of fraud scenario to inject
            base_transaction: Base transaction to modify
            
        Returns:
            List of transactions forming the fraud scenario
        """
        if scenario_name not in self.fraud_scenarios:
            logger.warning(f"Unknown fraud scenario: {scenario_name}")
            return [base_transaction]
        
        scenario_func = self.fraud_scenarios[scenario_name]
        return scenario_func(base_transaction)
    
    def _generate_account_takeover(self, base_transaction: Dict) -> List[Dict]:
        """
        Generate account takeover fraud pattern.
        Characteristics:
        - Sudden change in spending patterns
        - Geographic anomalies
        - Different merchant types
        - Higher than normal amounts
        """
        transactions = []
        customer_id = base_transaction["customer_id"]
        
        # Generate 5-15 fraudulent transactions in quick succession
        fraud_count = random.randint(5, 15)
        
        # Different locations from normal (international or distant domestic)
        fraud_locations = [
            {"country": "RU", "state": None, "city": "Moscow", "zip_code": None, "lat": 55.7558, "lon": 37.6176},
            {"country": "CN", "state": None, "city": "Shanghai", "zip_code": None, "lat": 31.2304, "lon": 121.4737},
            {"country": "NG", "state": None, "city": "Lagos", "zip_code": None, "lat": 6.5244, "lon": 3.3792},
            {"country": "US", "state": "FL", "city": "Miami", "zip_code": "33101", "lat": 25.7617, "lon": -80.1918},
        ]
        
        # High-risk merchant categories
        fraud_merchants = [
            ("ONLINE", "5734", "Suspicious Online Store", 0.9),
            ("RETAIL", "5999", "Electronics Liquidation", 0.8),
            ("OTHER", "9999", "Unknown Business", 0.95),
        ]
        
        base_time = datetime.fromtimestamp(base_transaction["timestamp"] / 1000)
        
        for i in range(fraud_count):
            # Transactions occur within 2 hours
            fraud_time = base_time + timedelta(minutes=random.randint(0, 120))
            
            fraud_location = random.choice(fraud_locations)
            category, mcc, merchant_name, risk_score = random.choice(fraud_merchants)
            
            fraud_transaction = base_transaction.copy()
            fraud_transaction.update({
                "transaction_id": str(uuid.uuid4()),
                "timestamp": int(fraud_time.timestamp() * 1000),
                "amount": str(random.uniform(500, 5000)),  # Higher amounts
                "merchant_id": f"fraud_merch_{str(uuid.uuid4())[:8]}",
                "merchant_name": merchant_name,
                "merchant_category": category,
                "merchant_category_code": mcc,
                "location": fraud_location,
                "is_international": fraud_location["country"] != "US",
                "entry_method": random.choice(["ONLINE", "MANUAL"]),  # Card-not-present
                "customer_present": False,
                "metadata": {
                    **fraud_transaction.get("metadata", {}),
                    "fraud_pattern": "account_takeover",
                    "fraud_sequence": i + 1,
                    "fraud_scenario_id": str(uuid.uuid4()),
                    "is_synthetic_fraud": "true",
                    "merchant_risk_score": str(risk_score)
                }
            })
            
            transactions.append(fraud_transaction)
        
        return transactions
    
    def _generate_card_testing(self, base_transaction: Dict) -> List[Dict]:
        """
        Generate card testing fraud pattern.
        Characteristics:
        - Multiple small amounts (under $5)
        - Same merchant or merchant type
        - Rapid succession
        - Mix of approved/declined
        """
        transactions = []
        base_time = datetime.fromtimestamp(base_transaction["timestamp"] / 1000)
        
        # 10-50 small test transactions
        test_count = random.randint(10, 50)
        
        for i in range(test_count):
            # Transactions occur within 30 minutes
            test_time = base_time + timedelta(seconds=random.randint(0, 1800))
            
            fraud_transaction = base_transaction.copy()
            fraud_transaction.update({
                "transaction_id": str(uuid.uuid4()),
                "timestamp": int(test_time.timestamp() * 1000),
                "amount": str(random.uniform(0.50, 4.99)),  # Small test amounts
                "status": random.choices(
                    ["APPROVED", "DECLINED"],
                    weights=[0.3, 0.7]  # More declines than normal
                )[0],
                "processor_response_code": random.choice(["00", "05", "14", "51", "61"]),
                "entry_method": "ONLINE",
                "customer_present": False,
                "metadata": {
                    **fraud_transaction.get("metadata", {}),
                    "fraud_pattern": "card_testing",
                    "fraud_sequence": i + 1,
                    "fraud_scenario_id": str(uuid.uuid4()),
                    "is_synthetic_fraud": "true"
                }
            })
            
            transactions.append(fraud_transaction)
        
        return transactions
    
    def _generate_velocity_attack(self, base_transaction: Dict) -> List[Dict]:
        """
        Generate velocity-based fraud attack.
        Characteristics:
        - High transaction frequency
        - Various amounts
        - Multiple merchants
        - Short time window
        """
        transactions = []
        base_time = datetime.fromtimestamp(base_transaction["timestamp"] / 1000)
        
        # 20-100 transactions in 1 hour
        velocity_count = random.randint(20, 100)
        
        for i in range(velocity_count):
            # All transactions within 1 hour
            velocity_time = base_time + timedelta(seconds=random.randint(0, 3600))
            
            fraud_transaction = base_transaction.copy()
            fraud_transaction.update({
                "transaction_id": str(uuid.uuid4()),
                "timestamp": int(velocity_time.timestamp() * 1000),
                "amount": str(random.uniform(50, 1000)),
                "merchant_id": f"velocity_merch_{random.randint(1, 10)}",
                "merchant_name": f"Quick Merchant {random.randint(1, 10)}",
                "entry_method": random.choice(["ONLINE", "MOBILE", "CONTACTLESS"]),
                "metadata": {
                    **fraud_transaction.get("metadata", {}),
                    "fraud_pattern": "velocity_attack",
                    "fraud_sequence": i + 1,
                    "fraud_scenario_id": str(uuid.uuid4()),
                    "is_synthetic_fraud": "true"
                }
            })
            
            transactions.append(fraud_transaction)
        
        return transactions
    
    def _generate_geographic_impossible(self, base_transaction: Dict) -> List[Dict]:
        """
        Generate geographically impossible transaction pattern.
        Characteristics:
        - Transactions in locations too far apart in short time
        - Impossible travel velocity between locations
        """
        transactions = []
        base_time = datetime.fromtimestamp(base_transaction["timestamp"] / 1000)
        
        # Impossible travel locations
        impossible_locations = [
            # Start in New York
            {"country": "US", "state": "NY", "city": "New York", "zip_code": "10001", "lat": 40.7128, "lon": -74.0060},
            # 30 minutes later in London (impossible)
            {"country": "GB", "state": None, "city": "London", "zip_code": None, "lat": 51.5074, "lon": -0.1278},
            # 1 hour later in Tokyo (impossible)
            {"country": "JP", "state": None, "city": "Tokyo", "zip_code": None, "lat": 35.6762, "lon": 139.6503},
            # 30 minutes later back in Los Angeles (impossible)
            {"country": "US", "state": "CA", "city": "Los Angeles", "zip_code": "90001", "lat": 34.0522, "lon": -118.2437},
        ]
        
        for i, location in enumerate(impossible_locations):
            # Impossible timing - 30-60 minutes between distant locations
            impossible_time = base_time + timedelta(minutes=(i * 45))
            
            fraud_transaction = base_transaction.copy()
            fraud_transaction.update({
                "transaction_id": str(uuid.uuid4()),
                "timestamp": int(impossible_time.timestamp() * 1000),
                "location": location,
                "is_international": location["country"] != "US",
                "amount": str(random.uniform(100, 800)),
                "metadata": {
                    **fraud_transaction.get("metadata", {}),
                    "fraud_pattern": "geographic_impossible",
                    "fraud_sequence": i + 1,
                    "fraud_scenario_id": str(uuid.uuid4()),
                    "is_synthetic_fraud": "true",
                    "impossible_travel": "true"
                }
            })
            
            transactions.append(fraud_transaction)
        
        return transactions
    
    def _generate_amount_probing(self, base_transaction: Dict) -> List[Dict]:
        """
        Generate amount probing pattern.
        Characteristics:
        - Testing different amounts to find limit
        - Incremental increases
        - Same merchant
        """
        transactions = []
        base_time = datetime.fromtimestamp(base_transaction["timestamp"] / 1000)
        
        # Start with small amount and increase
        probe_amounts = [10, 25, 50, 100, 250, 500, 1000, 2000, 5000]
        
        for i, amount in enumerate(probe_amounts):
            probe_time = base_time + timedelta(minutes=i * 5)
            
            fraud_transaction = base_transaction.copy()
            fraud_transaction.update({
                "transaction_id": str(uuid.uuid4()),
                "timestamp": int(probe_time.timestamp() * 1000),
                "amount": str(amount),
                "status": "APPROVED" if amount < 1000 else random.choice(["APPROVED", "DECLINED"]),
                "metadata": {
                    **fraud_transaction.get("metadata", {}),
                    "fraud_pattern": "amount_probing",
                    "fraud_sequence": i + 1,
                    "fraud_scenario_id": str(uuid.uuid4()),
                    "is_synthetic_fraud": "true",
                    "probe_amount": str(amount)
                }
            })
            
            transactions.append(fraud_transaction)
        
        return transactions
    
    def _generate_merchant_collusion(self, base_transaction: Dict) -> List[Dict]:
        """
        Generate merchant collusion fraud pattern.
        Characteristics:
        - Same customer, multiple related merchants
        - Similar amounts
        - Quick succession
        - High-risk merchant categories
        """
        transactions = []
        base_time = datetime.fromtimestamp(base_transaction["timestamp"] / 1000)
        
        # Related merchants (same owner/location)
        colluding_merchants = [
            ("RETAIL", "5999", "Electronics Store A", 0.8),
            ("RETAIL", "5999", "Electronics Store B", 0.8),
            ("ONLINE", "5734", "Online Electronics", 0.9),
            ("OTHER", "9999", "Cash Advance Service", 0.95),
        ]
        
        base_amount = random.uniform(200, 800)
        
        for i, (category, mcc, name, risk) in enumerate(colluding_merchants):
            collusion_time = base_time + timedelta(minutes=i * 15)
            
            fraud_transaction = base_transaction.copy()
            fraud_transaction.update({
                "transaction_id": str(uuid.uuid4()),
                "timestamp": int(collusion_time.timestamp() * 1000),
                "amount": str(base_amount + random.uniform(-50, 50)),  # Similar amounts
                "merchant_id": f"collude_merch_{i}",
                "merchant_name": name,
                "merchant_category": category,
                "merchant_category_code": mcc,
                "metadata": {
                    **fraud_transaction.get("metadata", {}),
                    "fraud_pattern": "merchant_collusion",
                    "fraud_sequence": i + 1,
                    "fraud_scenario_id": str(uuid.uuid4()),
                    "is_synthetic_fraud": "true",
                    "merchant_risk_score": str(risk)
                }
            })
            
            transactions.append(fraud_transaction)
        
        return transactions
    
    def _generate_synthetic_identity(self, base_transaction: Dict) -> List[Dict]:
        """
        Generate synthetic identity fraud pattern.
        Characteristics:
        - New customer with unrealistic spending
        - High amounts immediately
        - Multiple high-risk merchants
        """
        transactions = []
        base_time = datetime.fromtimestamp(base_transaction["timestamp"] / 1000)
        
        # Create synthetic customer ID
        synthetic_customer_id = f"synthetic_{str(uuid.uuid4())[:8]}"
        
        # High amounts for "new" customer
        synthetic_amounts = [1500, 2000, 2500, 3000, 1800, 2200]
        
        for i, amount in enumerate(synthetic_amounts):
            synthetic_time = base_time + timedelta(hours=i * 2)
            
            fraud_transaction = base_transaction.copy()
            fraud_transaction.update({
                "transaction_id": str(uuid.uuid4()),
                "timestamp": int(synthetic_time.timestamp() * 1000),
                "customer_id": synthetic_customer_id,
                "amount": str(amount),
                "merchant_category": random.choice(["RETAIL", "ONLINE", "OTHER"]),
                "metadata": {
                    **fraud_transaction.get("metadata", {}),
                    "fraud_pattern": "synthetic_identity",
                    "fraud_sequence": i + 1,
                    "fraud_scenario_id": str(uuid.uuid4()),
                    "is_synthetic_fraud": "true",
                    "synthetic_customer": "true"
                }
            })
            
            transactions.append(fraud_transaction)
        
        return transactions
    
    def _generate_bust_out_fraud(self, base_transaction: Dict) -> List[Dict]:
        """
        Generate bust-out fraud pattern.
        Characteristics:
        - Normal spending pattern initially
        - Sudden spike in spending
        - Multiple high-amount transactions
        - Then account abandonment
        """
        transactions = []
        base_time = datetime.fromtimestamp(base_transaction["timestamp"] / 1000)
        
        # Phase 1: Normal transactions (establish pattern)
        normal_amounts = [25, 45, 67, 89, 34, 56, 78]
        for i, amount in enumerate(normal_amounts):
            normal_time = base_time - timedelta(days=30-i*4)  # Over past month
            
            normal_transaction = base_transaction.copy()
            normal_transaction.update({
                "transaction_id": str(uuid.uuid4()),
                "timestamp": int(normal_time.timestamp() * 1000),
                "amount": str(amount),
                "metadata": {
                    **normal_transaction.get("metadata", {}),
                    "fraud_pattern": "bust_out_fraud",
                    "fraud_phase": "establishment",
                    "fraud_sequence": i + 1,
                    "fraud_scenario_id": str(uuid.uuid4()),
                    "is_synthetic_fraud": "true"
                }
            })
            
            transactions.append(normal_transaction)
        
        # Phase 2: Bust-out (sudden high amounts)
        bustout_amounts = [2500, 3000, 4000, 5000, 3500, 4500]
        for i, amount in enumerate(bustout_amounts):
            bustout_time = base_time + timedelta(hours=i)  # All in one day
            
            bust_transaction = base_transaction.copy()
            bust_transaction.update({
                "transaction_id": str(uuid.uuid4()),
                "timestamp": int(bustout_time.timestamp() * 1000),
                "amount": str(amount),
                "merchant_category": random.choice(["RETAIL", "ONLINE"]),
                "metadata": {
                    **bust_transaction.get("metadata", {}),
                    "fraud_pattern": "bust_out_fraud",
                    "fraud_phase": "bust_out",
                    "fraud_sequence": len(normal_amounts) + i + 1,
                    "fraud_scenario_id": str(uuid.uuid4()),
                    "is_synthetic_fraud": "true"
                }
            })
            
            transactions.append(bust_transaction)
        
        return transactions
    
    def get_random_fraud_scenario(self) -> str:
        """Get a random fraud scenario name."""
        return random.choice(list(self.fraud_scenarios.keys()))
    
    def get_fraud_scenario_description(self, scenario_name: str) -> str:
        """Get description of a fraud scenario."""
        descriptions = {
            "account_takeover": "Account compromise with sudden pattern changes",
            "card_testing": "Testing stolen card details with small amounts",
            "velocity_attack": "High-frequency transactions in short time",
            "geographic_impossible": "Transactions in impossible travel patterns",
            "amount_probing": "Testing transaction limits with incremental amounts",
            "merchant_collusion": "Coordinated fraud across related merchants",
            "synthetic_identity": "New identity with unrealistic spending patterns",
            "bust_out_fraud": "Establish normal pattern then maximum spending spree",
        }
        return descriptions.get(scenario_name, "Unknown fraud scenario")
