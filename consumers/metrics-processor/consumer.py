#!/usr/bin/env python3
"""
Metrics Processor Consumer
Consumes from router topics and generates aggregated metrics
"""

import json
import logging
import os
import re
import signal
import sys
import time
from collections import defaultdict
from datetime import datetime
from typing import Any, DefaultDict, Dict, Optional, Set

import psycopg2
from kafka import KafkaConsumer
from psycopg2.extensions import connection

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/var/log/consumer-metrics.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class MetricsProcessor:
    """Processes router logs and generates hourly/daily metrics"""

    TOPICS = [
        'router.events.raw',
        'router.security.raw',
        'router.control.raw',
    ]

    def __init__(self):
        """Initialize consumer and database connection"""
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        self.consumer_group = os.getenv('CONSUMER_GROUP_ID', 'metrics-processor')

        # Postgres connection params
        self.pg_params = {
            'host': os.getenv('POSTGRES_HOST', 'postgres'),
            'database': os.getenv('POSTGRES_DB', 'router_logs'),
            'user': os.getenv('POSTGRES_USER', 'router'),
            'password': os.getenv('POSTGRES_PASSWORD', 'router_password_change_me')
        }

        # In-memory accumulators (per hour)
        self.running = True
        self.current_hour = None
        self.wifi_metrics: DefaultDict[Any, Dict[str, Any]] = defaultdict(lambda: {
            'kickoffs': 0,
            'clients': set(),
            'btm_reports': 0,
            'btm_rejections': 0,
            'sta_leaves': 0
        })
        self.dhcp_metrics: DefaultDict[Any, Dict[str, Any]] = defaultdict(lambda: {
            'requests': 0,
            'assignments': 0,
            'clients': set(),
            'new_clients': set()
        })
        self.wan_metrics: DefaultDict[Any, Dict[str, int]] = defaultdict(lambda: {
            'events': 0,
            'auth_success': 0,
            'auth_fail': 0,
            'link_up': 0
        })
        self.system_metrics: DefaultDict[Any, Dict[str, int]] = defaultdict(lambda: {
            'timeouts': 0,
            'errors': 0,
            'ipv6_errors': 0
        })

        # Device tracking (daily)
        self.device_daily: DefaultDict[Any, Dict[str, Any]] = defaultdict(lambda: {
            'first_seen': None,
            'last_seen': None,
            'events': 0,
            'kickoffs': 0
        })

        self.consumer : KafkaConsumer = None
        self.db_conn: Optional[connection] = None

        # Signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.handle_exit)
        signal.signal(signal.SIGTERM, self.handle_exit)

    def connect_kafka(self):
        """Connect to Kafka"""
        max_retries = 10
        for attempt in range(max_retries):
            try:
                self.consumer = KafkaConsumer(
                    *self.TOPICS,
                    bootstrap_servers=self.kafka_servers,
                    group_id=self.consumer_group,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    auto_offset_reset='earliest',
                    enable_auto_commit=False,
                    max_poll_records=500
                )
                logger.info(f"Connected to Kafka at {self.kafka_servers}")
                return True
            except Exception as e:
                logger.warning(f"Kafka connection attempt {attempt+1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    raise
        return False

    def connect_postgres(self):
        """Connect to PostgreSQL"""
        max_retries = 10
        for attempt in range(max_retries):
            try:
                self.db_conn = psycopg2.connect(**self.pg_params)
                self.db_conn.autocommit = False
                logger.info(f"Connected to PostgreSQL at {self.pg_params['host']}")
                return True
            except Exception as e:
                logger.warning(f"Postgres connection attempt {attempt+1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    raise
        return False

    def get_hour_key(self, timestamp_str):
        """Get hour key from timestamp"""
        try:
            dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            return dt.replace(minute=0, second=0, microsecond=0)
        except:
            return datetime.now().replace(minute=0, second=0, microsecond=0)

    def extract_mac(self, message):
        """Extract MAC address from message"""
        # Pattern: kickoff sta:XX:XX:XX:XX:XX:XX
        match = re.search(r'([0-9a-f]{2}:[0-9a-f]{2}:[0-9a-f]{2}:[0-9a-f]{2}:[0-9a-f]{2}:[0-9a-f]{2})', message, re.I)
        if match:
            return match.group(1).lower()
        return None

    def process_wifi_log(self, log):
        """Process WiFi log for metrics"""
        hour = self.get_hour_key(log.get('@timestamp', ''))
        event_type = log.get('event_type', '')
        message = log.get('message', '')

        metrics = self.wifi_metrics[hour]

        # Extract MAC address
        mac = self.extract_mac(message)
        if mac:
            metrics['clients'].add(mac)

        # Count events
        if event_type == 'client_kickoff':
            metrics['kickoffs'] += 1
            if mac:
                # Track device daily
                date = hour.date()
                device_key = (date, mac)
                device = self.device_daily[device_key]
                device['kickoffs'] += 1
                device['events'] += 1
                device['last_seen'] = hour
                if device['first_seen'] is None:
                    device['first_seen'] = hour

        elif event_type == 'btm_report':
            metrics['btm_reports'] += 1
            # Status 7 = rejection
            if 'status[7]' in message or 'StatusCode=7' in message:
                metrics['btm_rejections'] += 1

        elif event_type == 'sta_report':
            metrics['sta_leaves'] += 1

    def process_dhcp_log(self, log):
        """Process DHCP log for metrics"""
        hour = self.get_hour_key(log.get('@timestamp', ''))
        event_type = log.get('event_type', '')
        message = log.get('message', '')

        metrics = self.dhcp_metrics[hour]

        # Extract MAC
        mac = self.extract_mac(message)
        if mac:
            metrics['clients'].add(mac)

        # Count events
        if event_type == 'request':
            metrics['requests'] += 1
        elif event_type == 'response':
            metrics['assignments'] += 1

    def process_wan_log(self, log):
        """Process WAN log for metrics"""
        hour = self.get_hour_key(log.get('@timestamp', ''))
        event_type = log.get('event_type', '')
        message = log.get('message', '')

        metrics = self.wan_metrics[hour]
        metrics['events'] += 1

        if event_type == 'ppp_auth':
            if 'success' in message.lower():
                metrics['auth_success'] += 1
            else:
                metrics['auth_fail'] += 1
        elif event_type == 'ppp_link_up':
            metrics['link_up'] += 1

    def process_system_log(self, log):
        """Process System log for metrics"""
        hour = self.get_hour_key(log.get('@timestamp', ''))
        event_type = log.get('event_type', '')

        metrics = self.system_metrics[hour]

        if event_type == 'timeout':
            metrics['timeouts'] += 1
        elif 'error' in event_type:
            metrics['errors'] += 1

    def process_ipv6_log(self, log):
        """Process IPv6 log for metrics"""
        hour = self.get_hour_key(log.get('@timestamp', ''))
        metrics = self.system_metrics[hour]
        metrics['ipv6_errors'] += 1

    def calculate_wifi_health_score(self, metrics):
        """Calculate WiFi health score (0-100)"""
        # Base score
        score = 100.0

        # Penalize for high kickoff rate
        unique_clients = len(metrics['clients'])
        if unique_clients > 0:
            kickoff_rate = metrics['kickoffs'] / unique_clients
            # Penalize 10 points per kickoff per client
            score -= min(kickoff_rate * 10, 50)

        # Penalize for BTM rejections
        if metrics['btm_reports'] > 0:
            rejection_rate = metrics['btm_rejections'] / metrics['btm_reports']
            score -= rejection_rate * 20

        # Penalize for frequent STA leaves
        if unique_clients > 0:
            leave_rate = metrics['sta_leaves'] / unique_clients
            score -= min(leave_rate * 5, 20)

        return max(0, min(100, score))

    def flush_hourly_metrics(self, hour):
        """Flush accumulated metrics to database"""
        if hour is None:
            return

        try:
            if self.db_conn.closed:
                self.connect_postgres()

            cursor = self.db_conn.cursor()

            # WiFi metrics
            if hour in self.wifi_metrics:
                wifi = self.wifi_metrics[hour]
                health_score = self.calculate_wifi_health_score(wifi)

                cursor.execute("""
                    INSERT INTO metrics_wifi_hourly (
                        hour, total_kickoffs, unique_clients, btm_reports,
                        btm_rejections, sta_leaves, health_score
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (hour) DO UPDATE SET
                        total_kickoffs = EXCLUDED.total_kickoffs,
                        unique_clients = EXCLUDED.unique_clients,
                        btm_reports = EXCLUDED.btm_reports,
                        btm_rejections = EXCLUDED.btm_rejections,
                        sta_leaves = EXCLUDED.sta_leaves,
                        health_score = EXCLUDED.health_score
                """, (
                    hour, wifi['kickoffs'], len(wifi['clients']),
                    wifi['btm_reports'], wifi['btm_rejections'],
                    wifi['sta_leaves'], health_score
                ))
                logger.info(f"Flushed WiFi metrics for {hour}: {wifi['kickoffs']} kickoffs, health={health_score:.1f}")

            # DHCP metrics
            if hour in self.dhcp_metrics:
                dhcp = self.dhcp_metrics[hour]
                cursor.execute("""
                    INSERT INTO metrics_dhcp_hourly (
                        hour, total_requests, total_assignments, unique_devices
                    ) VALUES (%s, %s, %s, %s)
                    ON CONFLICT (hour) DO UPDATE SET
                        total_requests = EXCLUDED.total_requests,
                        total_assignments = EXCLUDED.total_assignments,
                        unique_devices = EXCLUDED.unique_devices
                """, (
                    hour, dhcp['requests'], dhcp['assignments'], len(dhcp['clients'])
                ))
                logger.info(f"Flushed DHCP metrics for {hour}")

            # WAN metrics
            if hour in self.wan_metrics:
                wan = self.wan_metrics[hour]
                cursor.execute("""
                    INSERT INTO metrics_network_hourly (
                        hour, wan_events, ppp_auth_success, ppp_auth_fail, link_up_events
                    ) VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (hour) DO UPDATE SET
                        wan_events = EXCLUDED.wan_events,
                        ppp_auth_success = EXCLUDED.ppp_auth_success,
                        ppp_auth_fail = EXCLUDED.ppp_auth_fail,
                        link_up_events = EXCLUDED.link_up_events
                """, (
                    hour, wan['events'], wan['auth_success'],
                    wan['auth_fail'], wan['link_up']
                ))
                logger.info(f"Flushed WAN metrics for {hour}")

            # System metrics
            if hour in self.system_metrics:
                sys = self.system_metrics[hour]
                cursor.execute("""
                    INSERT INTO metrics_system_hourly (
                        hour, timeout_count, error_count, ipv6_errors
                    ) VALUES (%s, %s, %s, %s)
                    ON CONFLICT (hour) DO UPDATE SET
                        timeout_count = EXCLUDED.timeout_count,
                        error_count = EXCLUDED.error_count,
                        ipv6_errors = EXCLUDED.ipv6_errors
                """, (
                    hour, sys['timeouts'], sys['errors'], sys['ipv6_errors']
                ))
                logger.info(f"Flushed System metrics for {hour}")

            self.db_conn.commit()
            cursor.close()

            # Clear old hour data
            if hour in self.wifi_metrics:
                del self.wifi_metrics[hour]
            if hour in self.dhcp_metrics:
                del self.dhcp_metrics[hour]
            if hour in self.wan_metrics:
                del self.wan_metrics[hour]
            if hour in self.system_metrics:
                del self.system_metrics[hour]

        except Exception as e:
            logger.error(f"Error flushing metrics: {e}")
            self.db_conn.rollback()

    def flush_daily_device_metrics(self, date):
        """Flush daily device metrics"""
        try:
            cursor = self.db_conn.cursor()

            # Get devices for this date
            devices_to_flush = [
                (date, mac, data)
                for (d, mac), data in self.device_daily.items()
                if d == date
            ]

            if not devices_to_flush:
                return

            for date, mac, data in devices_to_flush:
                cursor.execute("""
                    INSERT INTO metrics_device_daily (
                        date, client_mac, first_seen, last_seen,
                        total_events, kickoff_count
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (date, client_mac) DO UPDATE SET
                        first_seen = LEAST(metrics_device_daily.first_seen, EXCLUDED.first_seen),
                        last_seen = GREATEST(metrics_device_daily.last_seen, EXCLUDED.last_seen),
                        total_events = metrics_device_daily.total_events + EXCLUDED.total_events,
                        kickoff_count = metrics_device_daily.kickoff_count + EXCLUDED.kickoff_count
                """, (
                    date, mac, data['first_seen'], data['last_seen'],
                    data['events'], data['kickoffs']
                ))

            self.db_conn.commit()
            cursor.close()

            logger.info(f"Flushed {len(devices_to_flush)} device metrics for {date}")

            # Clear flushed data
            for date, mac, _ in devices_to_flush:
                del self.device_daily[(date, mac)]

        except Exception as e:
            logger.error(f"Error flushing device metrics: {e}")
            self.db_conn.rollback()

    def process_message(self, message):
        """Process a single message"""
        try:
            log = message.value
            subsystem = log.get('subsystem', '')
            hour = self.get_hour_key(log.get('@timestamp', ''))

            # Initialize current hour
            if self.current_hour is None:
                self.current_hour = hour

            # If hour changed, flush old metrics
            if hour != self.current_hour:
                logger.info(f"Hour changed from {self.current_hour} to {hour}, flushing metrics")
                self.flush_hourly_metrics(self.current_hour)

                # Also flush daily device metrics for previous day
                if self.current_hour.date() != hour.date():
                    self.flush_daily_device_metrics(self.current_hour.date())

                self.current_hour = hour

            # Route to appropriate processor
            if subsystem == 'wifi':
                self.process_wifi_log(log)
            elif subsystem == 'dhcp':
                self.process_dhcp_log(log)
            elif subsystem == 'wan':
                self.process_wan_log(log)
            elif subsystem == 'system':
                self.process_system_log(log)
            elif subsystem == 'ipv6':
                self.process_ipv6_log(log)

        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def run(self):
        """Main consumer loop"""
        logger.info("Starting Metrics Processor Consumer...")

        self.connect_kafka()
        self.connect_postgres()

        logger.info("Consumer ready, processing messages...")

        try:
            while self.running:
                messages = self.consumer.poll(timeout_ms=1000)
                for tp, msgs in messages.items():
                    for message in msgs:
                        self.process_message(message)
                    self.consumer.commit()
        except Exception as e:
            logger.error(f"Fatal error: {e}", exc_info=True)
        finally:
            self.cleanup()

    def handle_exit(self, signum, frame):
        """Handle exit signals by setting running to False"""
        logger.info(f"Received signal {signum}. Starting graceful shutdown...")
        self.running = False

    def cleanup(self):
        """Clean shutdown - now called automatically on container stop"""
        logger.info("Final flush of metrics before exit...")

        # Flush remaining metrics
        if self.current_hour:
            self.flush_hourly_metrics(self.current_hour)
            self.flush_daily_device_metrics(self.current_hour.date())

        if self.consumer:
            self.consumer.close()

        if self.db_conn:
            self.db_conn.close()

        logger.info("Graceful shutdown complete.")


def main():
    """Entry point"""
    processor = MetricsProcessor()
    processor.run()


if __name__ == '__main__':
    main()
