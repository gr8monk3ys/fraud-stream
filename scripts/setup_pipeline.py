"""
Setup script for the Financial Transaction Streaming Pipeline.
Initializes infrastructure, creates topics, and sets up Snowflake.
"""
import logging
import sys
import os
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from src.config import settings
from src.utils.kafka_utils import KafkaManager
from src.utils.snowflake_utils import SnowflakeManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def setup_kafka():
    """Setup Kafka topics and schemas."""
    logger.info("Setting up Kafka infrastructure...")
    
    try:
        kafka_manager = KafkaManager()
        
        # Create topics
        success = kafka_manager.setup_streaming_topics()
        if success:
            logger.info("✅ Kafka topics created successfully")
        else:
            logger.error("❌ Failed to create some Kafka topics")
            return False
        
        # Register schemas
        try:
            transaction_schema_id = kafka_manager.register_schema(
                f"{settings.kafka.transactions_topic}-value",
                "src/schemas/transaction.avsc"
            )
            logger.info(f"✅ Transaction schema registered with ID: {transaction_schema_id}")
            
            fraud_alert_schema_id = kafka_manager.register_schema(
                f"{settings.kafka.fraud_alerts_topic}-value", 
                "src/schemas/fraud_alert.avsc"
            )
            logger.info(f"✅ Fraud alert schema registered with ID: {fraud_alert_schema_id}")
            
        except Exception as e:
            logger.warning(f"⚠️  Schema registration failed (may not be critical): {e}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Kafka setup failed: {e}")
        return False


def setup_snowflake():
    """Setup Snowflake database structure."""
    logger.info("Setting up Snowflake infrastructure...")
    
    try:
        snowflake_manager = SnowflakeManager()
        
        # Create database structure
        if snowflake_manager.create_database_structure():
            logger.info("✅ Snowflake database structure created")
        else:
            logger.error("❌ Failed to create database structure")
            return False
        
        # Create streaming tables
        if snowflake_manager.create_streaming_tables():
            logger.info("✅ Snowflake streaming tables created")
        else:
            logger.error("❌ Failed to create streaming tables")
            return False
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Snowflake setup failed: {e}")
        logger.info("💡 This may be expected if Snowflake credentials are not configured")
        return False


def validate_environment():
    """Validate environment configuration."""
    logger.info("Validating environment configuration...")
    
    # Check if required environment variables are set
    required_for_kafka = [
        "KAFKA_BOOTSTRAP_SERVERS",
        "SCHEMA_REGISTRY_URL"
    ]
    
    missing_kafka = []
    for var in required_for_kafka:
        if not os.getenv(var):
            missing_kafka.append(var)
    
    if missing_kafka:
        logger.warning(f"⚠️  Missing Kafka environment variables: {missing_kafka}")
        logger.info("💡 Using default local values for development")
    else:
        logger.info("✅ Kafka environment variables configured")
    
    # Check Snowflake (optional for local development)
    snowflake_vars = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
    missing_snowflake = [var for var in snowflake_vars if not os.getenv(var)]
    
    if missing_snowflake:
        logger.info(f"💡 Snowflake not configured (optional): missing {missing_snowflake}")
    else:
        logger.info("✅ Snowflake environment variables configured")
    
    return True


def main():
    """Main setup function."""
    logger.info("🚀 Starting Financial Transaction Streaming Pipeline Setup")
    
    # Validate environment
    if not validate_environment():
        logger.error("❌ Environment validation failed")
        return False
    
    # Setup Kafka
    kafka_success = setup_kafka()
    
    # Setup Snowflake (optional)
    snowflake_success = setup_snowflake()
    
    # Summary
    logger.info("\n" + "="*60)
    logger.info("📋 SETUP SUMMARY")
    logger.info("="*60)
    logger.info(f"Kafka Setup: {'✅ SUCCESS' if kafka_success else '❌ FAILED'}")
    logger.info(f"Snowflake Setup: {'✅ SUCCESS' if snowflake_success else '⚠️  SKIPPED/FAILED'}")
    
    if kafka_success:
        logger.info("\n🎉 Pipeline is ready for local development!")
        logger.info("\nNext steps:")
        logger.info("1. Start local Kafka: docker-compose up -d")
        logger.info("2. Generate transactions: python src/data_generator/transaction_producer.py") 
        logger.info("3. Run streaming pipeline: python src/streaming/transaction_processor.py")
    else:
        logger.error("\n❌ Setup incomplete. Please check errors above.")
        return False
    
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
