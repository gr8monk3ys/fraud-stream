# FraudStream

**Real-time financial transaction processing and fraud detection at scale.**

FraudStream is a production-grade streaming data pipeline that processes financial transactions in real-time, detecting fraudulent patterns before they cause damage. Built with Apache Kafka, Spark Structured Streaming, and Snowflake, it demonstrates how modern fintech companies protect millions of transactions per day.

## The Problem

Every second, financial institutions process thousands of credit card transactions. Hidden among legitimate purchases are fraudulent ones - account takeovers, card testing attacks, and synthetic identity fraud. Traditional batch processing catches these hours or days later. By then, the damage is done.

**FraudStream solves this by detecting fraud in milliseconds, not hours.**

## How It Works

```
                                    FraudStream Architecture

    [Transaction Sources]          [Stream Processing]           [Data Warehouse]

         Mobile App  ──┐
                       │
         POS Terminal ─┼──▶  Kafka  ──▶  Spark Streaming  ──▶  Snowflake
                       │      │              │                    │
         Online Store ─┘      │              │                    │
                              ▼              ▼                    ▼
                         Schema          Feature              Bronze/Silver/Gold
                         Registry        Engineering          Data Layers
                                             │
                                             ▼
                                      Fraud Detection
                                      - Velocity checks
                                      - Geographic anomalies
                                      - Amount patterns
                                      - Risk scoring
```

### The Data Flow

1. **Ingestion**: Transactions arrive via Kafka with Avro schema validation
2. **Validation**: Data quality checks ensure completeness and correctness
3. **Enrichment**: Features are calculated (velocity metrics, risk scores, anomaly detection)
4. **Security**: PII is masked using HMAC-SHA256 before storage
5. **Storage**: Data lands in Snowflake's medallion architecture (Bronze → Silver → Gold)
6. **Detection**: Suspicious patterns trigger real-time fraud alerts

## Key Features

### Real-Time Fraud Detection
- **8 fraud patterns** detected: account takeover, card testing, velocity attacks, geographic impossibility, amount probing, merchant collusion, synthetic identity, and bust-out fraud
- **Sub-second latency** from transaction to alert
- **ML-ready features** for model integration

### Production-Grade Data Quality
- Schema validation with Avro and Schema Registry
- Multi-level data quality checks (required fields, types, ranges, business rules)
- Quality scoring and trend monitoring
- Automated alerting on quality degradation

### Enterprise Security
- PII masking with salted HMAC-SHA256 hashing
- Fernet encryption for sensitive fields
- Comprehensive audit logging for compliance
- PCI DSS compliance patterns

### Scalable Architecture
- Designed for **10,000+ transactions per second**
- Horizontal scaling via Kafka partitions and Spark executors
- Medallion architecture for efficient query patterns
- Auto-scaling support for cloud deployment

## Quick Start

### Prerequisites
- Docker and Docker Compose
- Python 3.9+
- (Optional) Snowflake account for data warehouse features

### 1. Start the Infrastructure

```bash
# Clone the repository
git clone https://github.com/yourusername/fraudstream.git
cd fraudstream

# Start Kafka, Zookeeper, Schema Registry, and supporting services
docker-compose up -d

# Verify services are running
docker-compose ps
```

### 2. Set Up the Environment

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Copy and configure environment variables
cp .env.example .env
# Edit .env with your settings (Snowflake credentials are optional for local dev)
```

### 3. Initialize the Pipeline

```bash
# Create Kafka topics and register schemas
python scripts/setup_pipeline.py
```

### 4. Generate Transactions

```bash
# Start the transaction producer (generates realistic transaction data)
python -m src.data_generator.transaction_producer
```

### 5. Process Transactions

```bash
# In a new terminal, start the Spark streaming processor
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    src/streaming/transaction_processor.py
```

## Project Structure

```
fraudstream/
├── src/
│   ├── config/                 # Configuration management
│   │   ├── __init__.py
│   │   └── settings.py         # Pydantic settings with env var support
│   │
│   ├── data_generator/         # Transaction simulation
│   │   ├── __init__.py
│   │   ├── transaction_producer.py  # Generates realistic transactions
│   │   └── fraud_injector.py        # Injects fraud patterns for testing
│   │
│   ├── streaming/              # Spark Structured Streaming
│   │   ├── __init__.py
│   │   └── transaction_processor.py  # Main streaming pipeline
│   │
│   ├── schemas/                # Avro schema definitions
│   │   ├── transaction.avsc    # Transaction event schema
│   │   └── fraud_alert.avsc    # Fraud alert schema
│   │
│   └── utils/                  # Shared utilities
│       ├── __init__.py
│       ├── kafka_utils.py      # Kafka admin, producer, consumer
│       ├── snowflake_utils.py  # Snowflake connection and operations
│       ├── security.py         # PII masking, encryption, audit logging
│       └── data_quality.py     # Validation and quality monitoring
│
├── tests/                      # Test suite
│   ├── unit/                   # Unit tests for each module
│   ├── integration/            # Integration tests
│   └── conftest.py             # Shared test fixtures
│
├── scripts/
│   └── setup_pipeline.py       # Infrastructure initialization
│
├── docker-compose.yml          # Local development services
├── pyproject.toml              # Project config and tool settings
├── requirements.txt            # Python dependencies
└── .env.example                # Environment variable template
```

## Configuration

FraudStream uses environment variables for configuration. Copy `.env.example` to `.env` and customize:

```bash
# Core Settings
ENVIRONMENT=development
DEBUG_MODE=false

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
SCHEMA_REGISTRY_URL=http://localhost:8081
TRANSACTIONS_TOPIC=financial.transactions.raw
FRAUD_ALERTS_TOPIC=financial.fraud.alerts

# Snowflake (optional for local development)
SNOWFLAKE_ACCOUNT=your-account
SNOWFLAKE_USER=your-user
SNOWFLAKE_PASSWORD=your-password
SNOWFLAKE_DATABASE=FINANCIAL_DATA
SNOWFLAKE_WAREHOUSE=STREAMING_WH

# Security
CUSTOMER_ID_SALT=your-secret-salt-here
PII_MASKING_ENABLED=true
AUDIT_LOGGING_ENABLED=true

# Data Quality
SILVER_QUALITY_THRESHOLD=0.8
DATA_QUALITY_ALERT_THRESHOLD=0.95
```

## Fraud Detection Patterns

FraudStream detects these sophisticated fraud patterns:

| Pattern | Description | Detection Method |
|---------|-------------|------------------|
| **Account Takeover** | Compromised account with sudden behavior change | Geographic + spending anomalies |
| **Card Testing** | Testing stolen cards with small amounts | High-frequency small transactions |
| **Velocity Attack** | Rapid transaction bursts | Transactions per time window |
| **Geographic Impossible** | Transactions in impossible locations | Travel velocity calculation |
| **Amount Probing** | Testing transaction limits | Incremental amount patterns |
| **Merchant Collusion** | Coordinated fraudulent merchants | Related merchant analysis |
| **Synthetic Identity** | Fake identity with unrealistic patterns | Behavior anomaly detection |
| **Bust-Out Fraud** | Establish trust, then maximize fraud | Pattern change detection |

## Performance

| Metric | Local Development | Production Target |
|--------|-------------------|-------------------|
| Throughput | 1,000 TPS | 10,000+ TPS |
| Latency (P95) | < 2 seconds | < 500ms |
| Data Freshness | < 30 seconds | < 10 seconds |

## Development

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src --cov-report=html

# Run specific test file
pytest tests/unit/test_security.py -v
```

### Code Quality

```bash
# Format code
black src/ tests/

# Sort imports
isort src/ tests/

# Type checking
mypy src/

# Security scanning
bandit -r src/

# Lint
ruff check src/
```

## Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| Message Broker | Apache Kafka | High-throughput event streaming |
| Schema Management | Confluent Schema Registry | Avro schema validation |
| Stream Processing | Apache Spark Structured Streaming | Real-time data transformation |
| Data Warehouse | Snowflake | Scalable analytical storage |
| Configuration | Pydantic Settings | Type-safe configuration |
| Data Quality | Custom + Great Expectations | Validation framework |
| Security | HMAC-SHA256, Fernet | PII protection |

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes with tests
4. Run the test suite (`pytest`)
5. Run code quality checks (`black`, `isort`, `mypy`, `ruff`)
6. Commit your changes (`git commit -m 'Add amazing feature'`)
7. Push to the branch (`git push origin feature/amazing-feature`)
8. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

This project demonstrates production patterns used by leading fintech companies for real-time fraud detection. It's designed to showcase:

- **Real-time data engineering** with Kafka and Spark Streaming
- **Financial domain expertise** in fraud detection and risk management
- **Enterprise security practices** for handling sensitive financial data
- **Modern Python practices** with type hints, testing, and code quality tools

---

**Built for engineers who want to learn how real-time fraud detection works at scale.**
