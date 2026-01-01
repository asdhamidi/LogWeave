-- Router Logs Database Schema
-- Initialized automatically when Postgres container starts

\c router_logs;

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";  -- For text search

---------------------------------------------------
-- RAW LAYER: Store all logs as-is
---------------------------------------------------

CREATE TABLE IF NOT EXISTS raw_logs (
    id BIGSERIAL,
    timestamp TIMESTAMPTZ NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    host VARCHAR(100),
    source_ip INET NOT NULL,
    severity VARCHAR(20),
    facility VARCHAR(50),
    program VARCHAR(100),
    subsystem VARCHAR(50) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    message TEXT,
    raw_json JSONB,  -- Store complete log entry
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (id, timestamp)
) PARTITION BY RANGE (timestamp);

-- Create indexes on partitioned table
CREATE INDEX idx_raw_logs_timestamp ON raw_logs(timestamp DESC);
CREATE INDEX idx_raw_logs_subsystem ON raw_logs(subsystem);
CREATE INDEX idx_raw_logs_event_type ON raw_logs(event_type);
CREATE INDEX idx_raw_logs_source_ip ON raw_logs(source_ip);
CREATE INDEX idx_raw_logs_json ON raw_logs USING gin(raw_json);

-- Create initial partitions (current month + next month)
CREATE TABLE raw_logs_2025_12 PARTITION OF raw_logs
    FOR VALUES FROM ('2025-12-01') TO ('2026-01-01');

CREATE TABLE raw_logs_2026_01 PARTITION OF raw_logs
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');

-- Function to auto-create future partitions
CREATE OR REPLACE FUNCTION create_monthly_partition()
RETURNS void AS $$
DECLARE
    partition_date DATE;
    partition_name TEXT;
    start_date TEXT;
    end_date TEXT;
BEGIN
    -- Create partition for next 3 months
    FOR i IN 0..2 LOOP
        partition_date := DATE_TRUNC('month', CURRENT_DATE + (i || ' month')::INTERVAL);
        partition_name := 'raw_logs_' || TO_CHAR(partition_date, 'YYYY_MM');
        start_date := TO_CHAR(partition_date, 'YYYY-MM-DD');
        end_date := TO_CHAR(partition_date + INTERVAL '1 month', 'YYYY-MM-DD');

        -- Check if partition exists
        IF NOT EXISTS (
            SELECT 1 FROM pg_class WHERE relname = partition_name
        ) THEN
            EXECUTE format(
                'CREATE TABLE %I PARTITION OF raw_logs FOR VALUES FROM (%L) TO (%L)',
                partition_name, start_date, end_date
            );
            RAISE NOTICE 'Created partition: %', partition_name;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

---------------------------------------------------
-- TRANSFORMED LAYER: Parsed events with extracted fields
---------------------------------------------------

-- WiFi Client Kickoff Events
CREATE TABLE IF NOT EXISTS events_wifi_kickoff (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    client_mac MACADDR NOT NULL,
    bssid MACADDR,
    reason TEXT,
    raw_log_id BIGINT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_wifi_kickoff_timestamp ON events_wifi_kickoff(timestamp DESC);
CREATE INDEX idx_wifi_kickoff_client ON events_wifi_kickoff(client_mac);

-- WiFi BTM Reports
CREATE TABLE IF NOT EXISTS events_wifi_btm (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    client_mac MACADDR NOT NULL,
    source_bssid MACADDR,
    dest_bssid MACADDR,
    status_code INT,
    raw_log_id BIGINT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_wifi_btm_timestamp ON events_wifi_btm(timestamp DESC);
CREATE INDEX idx_wifi_btm_client ON events_wifi_btm(client_mac);

-- DHCP Assignments
CREATE TABLE IF NOT EXISTS events_dhcp_assignment (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    client_mac MACADDR NOT NULL,
    assigned_ip INET NOT NULL,
    lease_time INT,
    msgtype VARCHAR(50),
    raw_log_id BIGINT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_dhcp_timestamp ON events_dhcp_assignment(timestamp DESC);
CREATE INDEX idx_dhcp_client ON events_dhcp_assignment(client_mac);
CREATE INDEX idx_dhcp_ip ON events_dhcp_assignment(assigned_ip);

-- WAN/PPP Sessions
CREATE TABLE IF NOT EXISTS events_wan_session (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    event VARCHAR(50) NOT NULL,  -- auth, link_up, session_start, disconnect
    local_ip INET,
    remote_ip INET,
    raw_log_id BIGINT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_wan_timestamp ON events_wan_session(timestamp DESC);
CREATE INDEX idx_wan_event ON events_wan_session(event);

-- Authentication Events
CREATE TABLE IF NOT EXISTS events_auth (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    event VARCHAR(50) NOT NULL,  -- login, logout
    username VARCHAR(100),
    source_ip INET,
    success BOOLEAN,
    raw_log_id BIGINT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_auth_timestamp ON events_auth(timestamp DESC);
CREATE INDEX idx_auth_username ON events_auth(username);
CREATE INDEX idx_auth_source_ip ON events_auth(source_ip);

---------------------------------------------------
-- METRICS LAYER: Pre-aggregated analytics
---------------------------------------------------

-- WiFi Health Metrics (hourly)
CREATE TABLE IF NOT EXISTS metrics_wifi_hourly (
    hour TIMESTAMPTZ NOT NULL,
    total_kickoffs INT DEFAULT 0,
    unique_clients INT DEFAULT 0,
    btm_reports INT DEFAULT 0,
    btm_rejections INT DEFAULT 0,
    sta_leaves INT DEFAULT 0,
    health_score FLOAT,  -- 0-100 calculated score
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (hour)
);

CREATE INDEX idx_wifi_hourly_time ON metrics_wifi_hourly(hour DESC);

-- Device Presence (daily)
CREATE TABLE IF NOT EXISTS metrics_device_daily (
    date DATE NOT NULL,
    client_mac MACADDR NOT NULL,
    first_seen TIMESTAMPTZ,
    last_seen TIMESTAMPTZ,
    total_events INT DEFAULT 0,
    kickoff_count INT DEFAULT 0,
    disconnect_count INT DEFAULT 0,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (date, client_mac)
);

CREATE INDEX idx_device_daily_date ON metrics_device_daily(date DESC);
CREATE INDEX idx_device_daily_mac ON metrics_device_daily(client_mac);

-- Network Uptime (hourly)
CREATE TABLE IF NOT EXISTS metrics_network_hourly (
    hour TIMESTAMPTZ NOT NULL,
    wan_events INT DEFAULT 0,
    ppp_auth_success INT DEFAULT 0,
    ppp_auth_fail INT DEFAULT 0,
    link_up_events INT DEFAULT 0,
    disconnect_events INT DEFAULT 0,
    uptime_seconds INT,  -- Estimated uptime
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (hour)
);

CREATE INDEX idx_network_hourly_time ON metrics_network_hourly(hour DESC);

-- DHCP Activity (hourly)
CREATE TABLE IF NOT EXISTS metrics_dhcp_hourly (
    hour TIMESTAMPTZ NOT NULL,
    total_requests INT DEFAULT 0,
    total_assignments INT DEFAULT 0,
    unique_devices INT DEFAULT 0,
    new_devices INT DEFAULT 0,  -- First time seen
    queries INT DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (hour)
);

CREATE INDEX idx_dhcp_hourly_time ON metrics_dhcp_hourly(hour DESC);

-- System Health (hourly)
CREATE TABLE IF NOT EXISTS metrics_system_hourly (
    hour TIMESTAMPTZ NOT NULL,
    timeout_count INT DEFAULT 0,
    error_count INT DEFAULT 0,
    ipv6_errors INT DEFAULT 0,
    process_failures INT DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (hour)
);

CREATE INDEX idx_system_hourly_time ON metrics_system_hourly(hour DESC);

---------------------------------------------------
-- HELPER VIEWS
---------------------------------------------------

-- Current active devices (seen in last 24 hours)
CREATE OR REPLACE VIEW view_active_devices AS
SELECT DISTINCT ON (client_mac)
    client_mac,
    timestamp as last_seen,
    'wifi' as source
FROM events_wifi_kickoff
WHERE timestamp > NOW() - INTERVAL '24 hours'
UNION
SELECT DISTINCT ON (client_mac)
    client_mac,
    timestamp as last_seen,
    'dhcp' as source
FROM events_dhcp_assignment
WHERE timestamp > NOW() - INTERVAL '24 hours'
ORDER BY client_mac, last_seen DESC;

-- WiFi health summary (last 24 hours)
CREATE OR REPLACE VIEW view_wifi_health_24h AS
SELECT
    COUNT(*) as total_events,
    COUNT(DISTINCT client_mac) as unique_clients,
    COUNT(*) FILTER (WHERE timestamp > NOW() - INTERVAL '1 hour') as events_last_hour,
    AVG(health_score) as avg_health_score
FROM events_wifi_kickoff
WHERE timestamp > NOW() - INTERVAL '24 hours';

---------------------------------------------------
-- MAINTENANCE FUNCTIONS
---------------------------------------------------

-- Function to clean old partitions (keep 30 days)
CREATE OR REPLACE FUNCTION cleanup_old_partitions()
RETURNS void AS $$
DECLARE
    partition_name TEXT;
    cutoff_date DATE;
BEGIN
    cutoff_date := CURRENT_DATE - INTERVAL '30 days';

    FOR partition_name IN
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public'
        AND tablename LIKE 'raw_logs_%'
        AND tablename < 'raw_logs_' || TO_CHAR(cutoff_date, 'YYYY_MM')
    LOOP
        EXECUTE format('DROP TABLE IF EXISTS %I', partition_name);
        RAISE NOTICE 'Dropped old partition: %', partition_name;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO router;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO router;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO router;

-- Create monthly partitions on startup
SELECT create_monthly_partition();
