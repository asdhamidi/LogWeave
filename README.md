# LogWeave: A Self-Hosted Router Log Ingestion and Analytics Platform

This repository contains a **self‑hosted, end‑to‑end log ingestion, processing, storage, and visualization platform** designed around router/network logs. It is built to be **modular, observable, and production‑style**, while still being runnable locally using Docker Compose.

At a high level, the system:

- Receives raw router logs over **TCP (rsyslog → Python)**
- Buffers and distributes logs using **Kafka**
- Archives raw events into **PostgreSQL**
- Processes logs into **time‑series and analytical metrics**
- Visualizes system health using **Grafana dashboards**

---

## 1. Architecture Overview

```
Router
  │
  │ (syslog over TCP)
  ▼
rsyslog
  │
  │ JSON logs
  ▼
Python Producer (TCP Server)
  │
  │ Kafka messages
  ▼
Kafka Broker
  │
  ├── consumer-raw      → PostgreSQL (raw_logs)
  └── consumer-metrics  → PostgreSQL (aggregated metrics)

PostgreSQL
  │
  ▼
Grafana Dashboards
```

### Design principles

- **Separation of concerns**
- **Event streaming, not direct DB writes**
- **Idempotent consumers**
- **SQL‑driven analytics**
- **Infrastructure‑as‑code** (Grafana provisioning)

---

## 2. Core Components

### 2.1 rsyslog (outside Docker)

- Runs on the host machine
- Receives logs from the router
- Normalizes them into structured JSON
- Forwards logs via **TCP** to the Python producer container

Why rsyslog?

- Battle‑tested
- Handles burst traffic well
- Excellent parsing and templating

---

### 2.2 Python Producer (TCP log receiver)

**Container:** `router-producer`

Responsibilities:

- Listens on a TCP port (`10514`)
- Accepts rsyslog connections
- Reads newline‑delimited JSON log events
- Publishes logs to Kafka topics

Key characteristics:

- Stateless
- Does _not_ touch the database
- Back‑pressure handled by Kafka

Why not write directly to Postgres?

- Tight coupling
- Poor burst handling
- No replay capability

---

### 2.3 Kafka + Zookeeper

**Containers:**

- `router-zookeeper`
- `router-kafka`

Kafka acts as the **central event backbone**.

It provides:

- Decoupling between producers and consumers
- Replayability during development
- Independent scaling of consumers
- Fault isolation

Kafka is intentionally **ephemeral** in this setup:

- No volume persistence
- Postgres is the system of record

---

### 2.4 Kafka UI

**Container:** `router-kafka-ui`

Used for:

- Inspecting topics
- Viewing messages
- Debugging consumer offsets

This is a developer convenience tool and not required for runtime.

---

### 2.5 Consumer: Raw Archiver

**Container:** `router-consumer-raw`

Responsibilities:

- Consumes raw log events from Kafka
- Inserts logs into `raw_logs` table in PostgreSQL

Important behavior:

- Connects to Postgres **only when inserting**, not at startup
- Designed to survive DB restarts
- Stores raw data for audit and reprocessing

---

### 2.6 Consumer: Metrics Processor

**Container:** `router-consumer-metrics`

Responsibilities:

- Consumes the same Kafka logs
- Derives higher‑level metrics
- Writes into aggregated tables such as:
  - `metrics_wifi_hourly`
  - `metrics_device_daily`
  - `metrics_dhcp_hourly`
  - `metrics_system_hourly`

All heavy analytics are done in **SQL**, not Python.

---

### 2.7 PostgreSQL

**Container:** `router-postgres`

Role:

- Primary data store
- Holds both raw logs and derived metrics

Persistence:

- Data stored in Docker named volume `postgres_data`
- Survives container restarts and rebuilds

Initialization:

- `init.sql` runs only on first startup
- Defines schema, tables, indexes, partitions

Postgres is the **single source of truth**.

---

### 2.8 Grafana

**Container:** `router-grafana`

Role:

- Visualization layer
- Executes SQL directly against Postgres

Persistence:

- Grafana state stored in `grafana_data` volume
- Dashboards provisioned from JSON files

Provisioning:

- Datasources defined in `/etc/grafana/provisioning/datasources`
- Dashboards defined in `/etc/grafana/provisioning/dashboards`

Dashboards are treated as **code**, not UI artifacts.

---

## 3. Docker Networking

```yaml
networks:
  router-net:
    driver: bridge
```

- All services share a private Docker bridge network
- Containers communicate using service names (`postgres`, `kafka`, etc.)
- Only selected ports are exposed to the host

Benefits:

- Isolation
- Predictable DNS
- Production‑like behavior

---

## 4. Persistence Model

| Component          | Persistence | Mechanism              |
| ------------------ | ----------- | ---------------------- |
| PostgreSQL data    | Yes         | Named Docker volume    |
| Grafana state      | Yes         | Named Docker volume    |
| Grafana dashboards | Yes         | Git‑tracked JSON files |
| Kafka messages     | No          | Ephemeral              |
| Zookeeper state    | No          | Ephemeral              |

This keeps the system:

- Durable where it matters
- Lightweight where it doesn’t

---

## 5. Grafana Dashboards

The platform includes a comprehensive dashboard covering:

- WiFi health score
- Active devices
- WAN uptime
- Event volume
- DHCP behavior
- System errors
- Device‑level problem ranking

Dashboards:

- Are version‑controlled
- Auto‑loaded at startup
- Reproducible on any machine

---

## 6. Running the Stack

```bash
docker compose up -d
```

Then access:

- Grafana: [http://localhost:3000](http://localhost:3000)
  - user: `admin`
  - password: `admin`

- Kafka UI: [http://localhost:8080](http://localhost:8080)

---

## 7. Why This Architecture?

This system could technically be built using **only SQL and cron jobs**, but that would:

- Couple ingestion to storage
- Make burst handling difficult
- Prevent real‑time streaming
- Eliminate replay and fan‑out

Kafka + consumers give:

- Temporal decoupling
- Scalability
- Observability
- Clean separation between ingestion and analytics

---

## 8. Intended Use Cases

- Router / network diagnostics
- Home‑lab observability
- Edge log ingestion pipelines
- Data engineering portfolio project
- Streaming + SQL analytics practice

---

## 9. Summary

This project demonstrates:

- Streaming ingestion with Kafka
- Robust log handling with rsyslog
- SQL‑centric analytics
- Infrastructure‑as‑code dashboards
- Clean separation between data layers
