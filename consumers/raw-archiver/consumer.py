#!/usr/bin/env python3
"""
Raw Archiver Consumer
Consumes from all router.*.raw topics and archives to PostgreSQL raw_logs table
"""

import json
import logging
import os
import sys
import time
from datetime import datetime

import psycopg2
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from psycopg2.extras import execute_batch

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/var/log/consumer-raw.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class RawArchiver:
    """Archives all router logs to Postgres raw_logs table"""

    # All topics to subscribe to
    TOPICS = [
        'router.events.raw',
        'router.security.raw',
        'router.control.raw',
    ]

    def __init__(self):
        """Initialize consumer and database connection"""
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        self.consumer_group = os.getenv('CONSUMER_GROUP_ID', 'raw-archiver')

        # Postgres connection params
        self.pg_params = {
            'host': os.getenv('POSTGRES_HOST', 'postgres'),
            'database': os.getenv('POSTGRES_DB', 'router_logs'),
            'user': os.getenv('POSTGRES_USER', 'admin'),
            'password': os.getenv('POSTGRES_PASSWORD', 'admin')
        }

        self.batch_size = int(os.getenv('BATCH_SIZE', '100'))
        self.batch_timeout = int(os.getenv('BATCH_TIMEOUT', '5'))  # seconds

        self.stats = {
            'total': 0,
            'success': 0,
            'failed': 0,
            'batches': 0
        }

        self.consumer = None
        self.db_conn = None
        self.buffer = []
        self.last_flush = time.time()

    def connect_kafka(self):
        """Connect to Kafka with retries"""
        max_retries = 10
        for attempt in range(max_retries):
            try:
                self.consumer = KafkaConsumer(
                    *self.TOPICS,
                    bootstrap_servers=self.kafka_servers,
                    group_id=self.consumer_group,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    auto_offset_reset='earliest',  # Start from beginning
                    enable_auto_commit=False,  # Manual commit for reliability
                    max_poll_records=500,  # Fetch up to 500 records per poll
                    session_timeout_ms=30000,
                    heartbeat_interval_ms=10000
                )
                logger.info(f"Connected to Kafka at {self.kafka_servers}")
                logger.info(f"Subscribed to topics: {self.TOPICS}")
                return True
            except Exception as e:
                logger.warning(f"Kafka connection attempt {attempt+1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error("Failed to connect to Kafka after all retries")
                    raise
        return False

    def connect_postgres(self):
        """Connect to PostgreSQL with retries"""
        max_retries = 10
        for attempt in range(max_retries):
            try:
                self.db_conn = psycopg2.connect(**self.pg_params)
                self.db_conn.autocommit = False  # Use transactions
                logger.info(f"Connected to PostgreSQL at {self.pg_params['host']}")
                return True
            except Exception as e:
                logger.warning(f"Postgres connection attempt {attempt+1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    logger.error("Failed to connect to Postgres after all retries")
                    raise
        return False

    def insert_batch(self, records):
        """Insert a batch of records into Postgres"""
        if not records:
            return

        try:
            if not self.db_conn or self.db_conn.closed:
                self.connect_postgres()
            cursor = self.db_conn.cursor()

            # Prepare INSERT statement
            insert_query = """
                INSERT INTO raw_logs (
                    timestamp, generated_at, host, source_ip, severity,
                    facility, program, subsystem, event_type, message, raw_json
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """

            # Prepare batch data
            batch_data = []
            for record in records:
                try:
                    # Parse timestamp
                    timestamp = record.get('@timestamp')
                    generated_at = record.get('@generated', timestamp)

                    batch_data.append((
                        timestamp,
                        generated_at,
                        record.get('host'),
                        record.get('source_ip'),
                        record.get('severity'),
                        record.get('facility'),
                        record.get('program'),
                        record.get('subsystem'),
                        record.get('event_type'),
                        record.get('message'),
                        json.dumps(record)  # Store full JSON
                    ))
                except Exception as e:
                    logger.error(f"Error preparing record: {e}")
                    self.stats['failed'] += 1
                    continue

            # Execute batch insert
            execute_batch(cursor, insert_query, batch_data, page_size=100)
            self.db_conn.commit()
            cursor.close()

            self.stats['success'] += len(batch_data)
            self.stats['batches'] += 1

            logger.info(f"Inserted batch of {len(batch_data)} records")

        except Exception as e:
            logger.error(f"Batch insert failed: {e}")
            self.db_conn.rollback()
            self.stats['failed'] += len(records)

            # Try to reconnect
            try:
                self.db_conn.close()
            except:
                pass
            self.connect_postgres()

    def flush_buffer(self):
        """Flush buffered records to database"""
        if not self.buffer:
            return

        logger.debug(f"Flushing buffer with {len(self.buffer)} records")
        self.insert_batch(self.buffer)
        self.buffer = []
        self.last_flush = time.time()

    def should_flush(self):
        """Check if buffer should be flushed"""
        return (
            len(self.buffer) >= self.batch_size or
            (time.time() - self.last_flush) >= self.batch_timeout
        )

    def process_message(self, message):
        """Process a single Kafka message"""
        logger.info("Processing a message...")
        try:
            self.stats['total'] += 1

            # Add to buffer
            self.buffer.append(message.value)

            # Flush if needed
            if self.should_flush():
                self.flush_buffer()
                # Commit Kafka offset after successful flush
                self.consumer.commit()

            # Log stats every 1000 messages
            if self.stats['total'] % 1000 == 0:
                self.log_stats()

        except Exception as e:
            logger.error(f"Error processing message: {e}")
            self.stats['failed'] += 1

    def log_stats(self):
        """Log processing statistics"""
        success_rate = (self.stats['success'] / self.stats['total'] * 100) if self.stats['total'] > 0 else 0
        logger.info(
            f"Stats: Total={self.stats['total']}, "
            f"Success={self.stats['success']}, "
            f"Failed={self.stats['failed']}, "
            f"Batches={self.stats['batches']}, "
            f"Success Rate={success_rate:.2f}%"
        )

    def run(self):
        """Main consumer loop"""
        logger.info("Starting Raw Archiver Consumer...")

        # Connect to services
        self.connect_kafka()
        self.connect_postgres()

        logger.info("Consumer ready, waiting for messages...")

        try:
            # Main consumption loop
            for message in self.consumer:
                self.process_message(message)

        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
        except Exception as e:
            logger.error(f"Fatal error in consumer loop: {e}", exc_info=True)
            raise
        finally:
            self.cleanup()

    def cleanup(self):
        """Clean shutdown"""
        logger.info("Shutting down consumer...")

        # Flush any remaining buffered records
        if self.buffer:
            logger.info(f"Flushing {len(self.buffer)} remaining records")
            self.flush_buffer()

        # Commit final offsets
        if self.consumer:
            try:
                self.consumer.commit()
            except:
                pass

        # Log final stats
        self.log_stats()

        # Close connections
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer closed")

        if self.db_conn:
            self.db_conn.close()
            logger.info("Postgres connection closed")

        logger.info("Consumer shutdown complete")


def main():
    """Entry point"""
    archiver = RawArchiver()
    archiver.run()


if __name__ == '__main__':
    main()
