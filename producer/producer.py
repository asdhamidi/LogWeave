#!/usr/bin/env python3
"""
Router Log Kafka Producer - TCP Socket Server
Listens on TCP port, receives JSON logs from rsyslog, publishes to Kafka
"""

import json
import logging
import os
import socket
import sys
import threading
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class RouterLogProducer:
    """Kafka producer for router logs - TCP socket server"""

    # Topic mapping based on subsystem
    TOPIC_MAP = {
        # High-volume operational events
        'dhcp': 'router.events.raw',
        'wifi': 'router.events.raw',
        'lan': 'router.events.raw',
        'dns': 'router.events.raw',
        'ipv6': 'router.events.raw',
        'qos': 'router.events.raw',
        'voip': 'router.events.raw',
        'usb': 'router.events.raw',
        'network': 'router.events.raw',

        # Security-sensitive
        'security': 'router.security.raw',
        'auth': 'router.security.raw',

        # Control / device state
        'tr069': 'router.control.raw',
        'wan': 'router.control.raw',
        'system': 'router.control.raw',
        'unknown': 'router.control.raw',
    }


    def __init__(self, kafka_servers='kafka:9092', listen_port=10514):
        """Initialize Kafka producer and TCP server"""
        self.listen_port = listen_port
        self.stats = {
            'total': 0,
            'success': 0,
            'failed': 0,
            'by_topic': {}
        }

        # Initialize Kafka producer with retries
        logger.info(f"Connecting to Kafka at {kafka_servers}...")
        max_retries = 10
        for attempt in range(max_retries):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=kafka_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    acks='all',
                    retries=3,
                    max_in_flight_requests_per_connection=5,
                    compression_type='gzip',
                    linger_ms=10,
                    batch_size=16384
                )
                logger.info(f"✓ Connected to Kafka at {kafka_servers}")
                break
            except Exception as e:
                logger.warning(f"Kafka connection attempt {attempt+1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    logger.error("Failed to connect to Kafka after all retries")
                    raise

    def get_topic(self, log_entry):
        """Determine Kafka topic based on log subsystem"""
        subsystem = log_entry.get('subsystem', 'unknown')
        return self.TOPIC_MAP.get(subsystem, 'router.unknown.raw')

    def get_key(self, log_entry):
        """Generate partition key"""
        return log_entry.get('source_ip', 'unknown')

    def send_log(self, log_entry):
        """Send log entry to appropriate Kafka topic"""
        try:
            topic = self.get_topic(log_entry)
            key = self.get_key(log_entry)

            future = self.producer.send(topic, key=key, value=log_entry)
            record_metadata = future.get(timeout=10)

            self.stats['success'] += 1
            self.stats['by_topic'][topic] = self.stats['by_topic'].get(topic, 0) + 1

            logger.debug(
                f"→ {topic} (partition {record_metadata.partition}, "
                f"offset {record_metadata.offset})"
            )
            return True

        except Exception as e:
            logger.error(f"Kafka error: {e}")
            self.stats['failed'] += 1
            return False

    def process_line(self, line):
        """Process a single log line"""
        self.stats['total'] += 1

        try:
            log_entry = json.loads(line.strip())

            if not log_entry.get('subsystem'):
                logger.warning(f"Missing subsystem: {line[:100]}")
                return False

            return self.send_log(log_entry)

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON: {e}")
            self.stats['failed'] += 1
            return False
        except Exception as e:
            logger.error(f"Error: {e}")
            self.stats['failed'] += 1
            return False

    def handle_client(self, client_socket, address):
        """Handle incoming client connection"""
        logger.info(f"Connection from {address}")

        try:
            buffer = ""
            while True:
                data = client_socket.recv(4096)
                if not data:
                    break

                buffer += data.decode('utf-8')

                # Process complete lines
                while '\n' in buffer:
                    line, buffer = buffer.split('\n', 1)
                    if line.strip():
                        self.process_line(line)

                # Log stats every 1000 messages
                if self.stats['total'] % 1000 == 0:
                    self.log_stats()

        except Exception as e:
            logger.error(f"Client handler error: {e}")
        finally:
            client_socket.close()
            logger.info(f"Connection closed from {address}")

    def log_stats(self):
        """Log processing statistics"""
        success_rate = (self.stats['success'] / self.stats['total'] * 100) if self.stats['total'] > 0 else 0
        logger.info(
            f"📊 Stats: Total={self.stats['total']}, "
            f"Success={self.stats['success']}, "
            f"Failed={self.stats['failed']}, "
            f"Rate={success_rate:.2f}%"
        )

    def run(self):
        """Main TCP server loop"""
        logger.info(f"Starting TCP server on port {self.listen_port}...")

        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind(('0.0.0.0', self.listen_port))
        server_socket.listen(5)

        logger.info(f"✓ TCP server listening on 0.0.0.0:{self.listen_port}")
        logger.info("Waiting for connections from rsyslog...")

        try:
            while True:
                client_socket, address = server_socket.accept()
                # Handle each connection in a separate thread
                client_thread = threading.Thread(
                    target=self.handle_client,
                    args=(client_socket, address)
                )
                client_thread.daemon = True
                client_thread.start()

        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
        except Exception as e:
            logger.error(f"Fatal error: {e}")
            raise
        finally:
            server_socket.close()
            self.cleanup()

    def cleanup(self):
        """Clean shutdown"""
        logger.info("Shutting down producer...")
        self.log_stats()
        self.producer.flush(timeout=30)
        self.producer.close()
        logger.info("Producer shutdown complete")


def main():
    """Entry point"""
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    listen_port = int(os.getenv('LISTEN_PORT', '5140'))

    producer = RouterLogProducer(
        kafka_servers=kafka_servers,
        listen_port=listen_port
    )
    producer.run()


if __name__ == '__main__':
    main()
