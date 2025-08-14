"""
Kafka utilities for producer and consumer operations.
Handles Kafka connections, serialization, and message handling.
"""
import json
import logging
from typing import Dict, List, Optional, Callable, Any
from contextlib import contextmanager

from confluent_kafka import Producer, Consumer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import (
    SerializationContext, 
    MessageField, 
    StringSerializer, 
    StringDeserializer
)

from src.config import settings

logger = logging.getLogger(__name__)


class KafkaManager:
    """Central manager for Kafka operations including admin tasks."""
    
    def __init__(self):
        """Initialize Kafka manager."""
        self.settings = settings
        self.admin_client = self._create_admin_client()
        self.schema_registry_client = self._create_schema_registry_client()
    
    def _create_admin_client(self) -> AdminClient:
        """Create Kafka admin client."""
        config = {
            'bootstrap.servers': self.settings.kafka.bootstrap_servers,
            'security.protocol': self.settings.kafka.security_protocol,
        }
        
        if self.settings.kafka.sasl_username:
            config.update({
                'sasl.mechanism': self.settings.kafka.sasl_mechanism,
                'sasl.username': self.settings.kafka.sasl_username,
                'sasl.password': self.settings.kafka.sasl_password,
            })
        
        return AdminClient(config)
    
    def _create_schema_registry_client(self) -> SchemaRegistryClient:
        """Create Schema Registry client."""
        config = {'url': self.settings.kafka.schema_registry_url}
        
        if self.settings.kafka.schema_registry_username:
            config['basic.auth.user.info'] = (
                f"{self.settings.kafka.schema_registry_username}:"
                f"{self.settings.kafka.schema_registry_password}"
            )
        
        return SchemaRegistryClient(config)
    
    def create_topics(self, topic_configs: List[Dict[str, Any]]) -> Dict[str, bool]:
        """
        Create Kafka topics if they don't exist.
        
        Args:
            topic_configs: List of topic configuration dictionaries
            
        Returns:
            Dictionary of topic names and creation success status
        """
        new_topics = []
        for config in topic_configs:
            topic = NewTopic(
                topic=config['name'],
                num_partitions=config.get('num_partitions', 3),
                replication_factor=config.get('replication_factor', 1),
                config=config.get('config', {})
            )
            new_topics.append(topic)
        
        # Create topics
        futures = self.admin_client.create_topics(new_topics)
        results = {}
        
        for topic_name, future in futures.items():
            try:
                future.result()  # Block until topic is created
                results[topic_name] = True
                logger.info(f"Successfully created topic: {topic_name}")
            except Exception as e:
                if "already exists" in str(e):
                    results[topic_name] = True
                    logger.info(f"Topic already exists: {topic_name}")
                else:
                    results[topic_name] = False
                    logger.error(f"Failed to create topic {topic_name}: {e}")
        
        return results
    
    def list_topics(self) -> Dict[str, Any]:
        """List all topics."""
        metadata = self.admin_client.list_topics(timeout=10)
        return {
            topic: {
                'partitions': len(topic_metadata.partitions),
                'replicas': len(list(topic_metadata.partitions.values())[0].replicas) if topic_metadata.partitions else 0
            }
            for topic, topic_metadata in metadata.topics.items()
        }
    
    def register_schema(self, subject: str, schema_path: str) -> int:
        """
        Register Avro schema with Schema Registry.
        
        Args:
            subject: Subject name for the schema
            schema_path: Path to the Avro schema file
            
        Returns:
            Schema ID
        """
        try:
            with open(schema_path, 'r') as f:
                schema_str = f.read()
            
            schema = Schema(schema_str, schema_type="AVRO")
            schema_id = self.schema_registry_client.register_schema(subject, schema)
            
            logger.info(f"Registered schema {subject} with ID: {schema_id}")
            return schema_id
            
        except Exception as e:
            logger.error(f"Failed to register schema {subject}: {e}")
            raise
    
    def setup_streaming_topics(self) -> bool:
        """Setup all required topics for the streaming pipeline."""
        topic_configs = [
            {
                'name': self.settings.kafka.transactions_topic,
                'num_partitions': 6,
                'replication_factor': 1,
                'config': {
                    'cleanup.policy': 'delete',
                    'retention.ms': '604800000',  # 7 days
                    'compression.type': 'snappy'
                }
            },
            {
                'name': self.settings.kafka.fraud_alerts_topic,
                'num_partitions': 3,
                'replication_factor': 1,
                'config': {
                    'cleanup.policy': 'delete',
                    'retention.ms': '2592000000',  # 30 days
                    'compression.type': 'snappy'
                }
            },
            {
                'name': self.settings.kafka.dlq_topic,
                'num_partitions': 1,
                'replication_factor': 1,
                'config': {
                    'cleanup.policy': 'delete',
                    'retention.ms': '2592000000',  # 30 days
                }
            }
        ]
        
        results = self.create_topics(topic_configs)
        success = all(results.values())
        
        if success:
            logger.info("All streaming topics created successfully")
        else:
            logger.error("Some topics failed to create")
        
        return success


class KafkaProducer:
    """High-level Kafka producer with serialization support."""
    
    def __init__(self, value_serializer: Optional[Any] = None, key_serializer: Optional[Any] = None):
        """
        Initialize Kafka producer.
        
        Args:
            value_serializer: Serializer for message values
            key_serializer: Serializer for message keys
        """
        self.settings = settings
        self.producer = self._create_producer()
        self.value_serializer = value_serializer or StringSerializer('utf_8')
        self.key_serializer = key_serializer or StringSerializer('utf_8')
        
        self.delivery_reports = []
    
    def _create_producer(self) -> Producer:
        """Create Kafka producer instance."""
        config = self.settings.get_kafka_config()
        config.update({
            'client.id': 'financial-streaming-producer',
            'acks': 'all',
            'retries': 3,
            'retry.backoff.ms': 100,
            'linger.ms': 5,
            'batch.size': 16384,
            'compression.type': 'snappy',
            'enable.idempotence': True,
        })
        
        return Producer(config)
    
    def produce(
        self, 
        topic: str, 
        value: Any, 
        key: Optional[Any] = None,
        partition: Optional[int] = None,
        callback: Optional[Callable] = None
    ):
        """
        Produce a message to Kafka topic.
        
        Args:
            topic: Target topic
            value: Message value
            key: Message key (optional)
            partition: Target partition (optional)
            callback: Delivery callback (optional)
        """
        try:
            # Serialize key and value
            serialization_context = SerializationContext(topic, MessageField.VALUE)
            
            serialized_value = self.value_serializer(value, serialization_context) if value else None
            serialized_key = self.key_serializer(key, SerializationContext(topic, MessageField.KEY)) if key else None
            
            # Produce message
            self.producer.produce(
                topic=topic,
                value=serialized_value,
                key=serialized_key,
                partition=partition,
                callback=callback or self._default_delivery_callback
            )
            
        except Exception as e:
            logger.error(f"Failed to produce message to {topic}: {e}")
            raise
    
    def _default_delivery_callback(self, err, msg):
        """Default delivery report callback."""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
            self.delivery_reports.append({
                'status': 'failed',
                'topic': msg.topic() if msg else None,
                'error': str(err),
                'timestamp': None
            })
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
            self.delivery_reports.append({
                'status': 'success',
                'topic': msg.topic(),
                'partition': msg.partition(),
                'offset': msg.offset(),
                'timestamp': msg.timestamp()
            })
    
    def flush(self, timeout: float = 30.0):
        """Flush all pending messages."""
        self.producer.flush(timeout)
    
    def close(self):
        """Close the producer."""
        self.flush()
        self.producer = None
    
    def get_delivery_reports(self) -> List[Dict]:
        """Get delivery reports and clear the buffer."""
        reports = self.delivery_reports.copy()
        self.delivery_reports.clear()
        return reports


class KafkaConsumer:
    """High-level Kafka consumer with deserialization support."""
    
    def __init__(
        self, 
        topics: List[str], 
        group_id: str,
        value_deserializer: Optional[Any] = None,
        key_deserializer: Optional[Any] = None
    ):
        """
        Initialize Kafka consumer.
        
        Args:
            topics: List of topics to subscribe to
            group_id: Consumer group ID
            value_deserializer: Deserializer for message values
            key_deserializer: Deserializer for message keys
        """
        self.settings = settings
        self.topics = topics
        self.group_id = group_id
        self.consumer = self._create_consumer()
        self.value_deserializer = value_deserializer or StringDeserializer('utf_8')
        self.key_deserializer = key_deserializer or StringDeserializer('utf_8')
        
        # Subscribe to topics
        self.consumer.subscribe(topics)
    
    def _create_consumer(self) -> Consumer:
        """Create Kafka consumer instance."""
        config = self.settings.get_kafka_config()
        config.update({
            'group.id': self.group_id,
            'client.id': f'financial-streaming-consumer-{self.group_id}',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 5000,
            'max.poll.interval.ms': 300000,
            'session.timeout.ms': 30000,
            'heartbeat.interval.ms': 10000,
        })
        
        return Consumer(config)
    
    def consume(self, timeout: float = 1.0) -> Optional[Dict]:
        """
        Consume a single message.
        
        Args:
            timeout: Polling timeout in seconds
            
        Returns:
            Deserialized message or None
        """
        try:
            msg = self.consumer.poll(timeout)
            
            if msg is None:
                return None
            
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                return None
            
            # Deserialize message
            serialization_context_value = SerializationContext(msg.topic(), MessageField.VALUE)
            serialization_context_key = SerializationContext(msg.topic(), MessageField.KEY)
            
            value = self.value_deserializer(msg.value(), serialization_context_value) if msg.value() else None
            key = self.key_deserializer(msg.key(), serialization_context_key) if msg.key() else None
            
            return {
                'topic': msg.topic(),
                'partition': msg.partition(),
                'offset': msg.offset(),
                'timestamp': msg.timestamp(),
                'key': key,
                'value': value,
                'headers': dict(msg.headers()) if msg.headers() else {}
            }
            
        except Exception as e:
            logger.error(f"Error consuming message: {e}")
            return None
    
    def consume_batch(self, batch_size: int = 100, timeout: float = 1.0) -> List[Dict]:
        """
        Consume a batch of messages.
        
        Args:
            batch_size: Maximum number of messages to consume
            timeout: Polling timeout in seconds
            
        Returns:
            List of deserialized messages
        """
        messages = []
        
        for _ in range(batch_size):
            msg = self.consume(timeout)
            if msg is None:
                break
            messages.append(msg)
        
        return messages
    
    def commit(self, asynchronous: bool = True):
        """Commit current offsets."""
        try:
            self.consumer.commit(asynchronous=asynchronous)
        except Exception as e:
            logger.error(f"Failed to commit offsets: {e}")
    
    def close(self):
        """Close the consumer."""
        if self.consumer:
            self.consumer.close()
            self.consumer = None


@contextmanager
def kafka_producer(value_serializer=None, key_serializer=None):
    """Context manager for Kafka producer."""
    producer = KafkaProducer(value_serializer, key_serializer)
    try:
        yield producer
    finally:
        producer.close()


@contextmanager
def kafka_consumer(topics, group_id, value_deserializer=None, key_deserializer=None):
    """Context manager for Kafka consumer."""
    consumer = KafkaConsumer(topics, group_id, value_deserializer, key_deserializer)
    try:
        yield consumer
    finally:
        consumer.close()
