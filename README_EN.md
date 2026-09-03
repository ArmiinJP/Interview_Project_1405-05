---

# Real-Time Price Calculation Platform

A **real-time price calculation platform** designed to process high-volume shopping-cart requests using an event-driven architecture based on **FastAPI, Apache Kafka, Spark Structured Streaming, ClickHouse, and Locust**.

The project focuses on building a decoupled real-time processing pipeline, evaluating its behavior under different traffic patterns, identifying performance bottlenecks, and improving the processing layer without changing the core system architecture.

All major components are containerized and can be deployed using Docker Compose.

---

## 1. Architecture

The system follows a **single-node, event-driven architecture**:

```text
Locust
   │
   │ HTTP POST
   ▼
FastAPI
   │
   │ Async Publish
   ▼
Kafka
   │
   │ Consume
   ▼
Spark Structured Streaming
   │
   ├── Product Price
   ├── Shipping Fee
   ├── Tax
   ├── Discount
   └── Final Price
   │
   │ Publish Result
   ▼
Kafka
   │
   │ Consume
   ▼
FastAPI
   │
   ▼
Locust
```

FastAPI is responsible for receiving requests, generating a unique `request_id`, publishing requests to Kafka, and resolving the final result.

The actual price calculation is performed by **Spark Structured Streaming**, rather than inside the API.

Kafka acts as the asynchronous communication and buffering layer between the API and Spark.

### Architecture Documentation

The complete architecture diagrams are available under:

```text
Architecture/
├── Base.png
└── Flows.png
```

* `Base.png` provides the overall system architecture.
* `Flows.png` illustrates the main Request/Response, Reference Data, and Monitoring flows.

---

# 2. Main Components

| Component                      | Responsibility                                    |
| ------------------------------ | ------------------------------------------------- |
| **Locust**                     | Load generation and performance testing           |
| **FastAPI**                    | HTTP API, request management, and result delivery |
| **Kafka**                      | Asynchronous communication and buffering          |
| **Spark Structured Streaming** | Micro-batch processing and price calculation      |
| **ClickHouse**                 | Reference data and monitoring data storage        |
| **Prometheus**                 | Metrics collection                                |
| **Grafana**                    | Metrics visualization and dashboards              |
| **cAdvisor**                   | Container-level resource monitoring               |
| **Node Exporter**              | Host-level resource monitoring                    |
| **AKHQ**                       | Kafka monitoring and inspection                   |

The implementation of the individual components is available under:

```text
Source/
```

---

# 3. Price Calculation Flow

Each request contains shopping-cart information together with attributes such as country, city, and an optional promotion code.

Spark processes each micro-batch through the following steps:

1. Calculate the total product price.
2. Calculate the total product weight.
3. Calculate the transportation fee.
4. Calculate tax.
5. Calculate the discount.
6. Calculate the final price.
7. Publish the result to Kafka.

The calculation uses reference data stored in ClickHouse:

* `tax_rates`
* `shipping_fees`
* `promotions`

Reference data is loaded by Spark and used during the price calculation process.

---

# 4. Why Kafka + Spark?

The price calculation logic is intentionally separated from the API layer.

Instead of making FastAPI directly execute the business calculation for every incoming request, requests are published asynchronously to Kafka and processed by Spark Structured Streaming.

This provides several advantages:

* Keeps the API layer lightweight.
* Decouples request handling from business processing.
* Provides buffering during traffic spikes.
* Allows Spark to process requests in micro-batches.
* Makes each part of the pipeline independently observable and measurable.

The API therefore focuses on request/response management, while Spark is responsible for the computational workload.

---

# 5. Performance Analysis and Optimization

After implementing the initial version, the complete pipeline was tested under increasing load.

The first step was not to immediately modify the architecture, but to identify where the actual bottleneck was.

During the **Normal scenario**, multiple potential bottlenecks were considered, including:

* API processing
* Kafka throughput
* Spark micro-batch processing
* Reference-data access
* Join operations
* Shuffle and partitioning
* CPU and memory utilization
* Network traffic
* End-to-end response time

The collected metrics from Locust, Spark UI, Grafana, and container-level monitoring were then used to narrow down the bottleneck.

The Spark processing layer became one of the main areas of investigation because increasing load resulted in higher **Batch Duration** and increased processing latency.

The optimization was therefore performed incrementally rather than by changing the overall architecture.

Several Spark-level aspects were reviewed:

* Join cost
* Reference-data loading
* Broadcast joins
* Partitioning and parallelism
* Shuffle overhead
* DataFrame transformations
* Micro-batch processing
* Calculation structure
* Spark configuration

One of the important improvements was reducing the overhead associated with reference-data joins. Since the reference datasets are small lookup-style datasets, the processing was redesigned to avoid unnecessary distributed shuffle operations.

Spark configuration and the processing pipeline were also tuned to improve **Process Rate** and reduce **Batch Duration**.

The resulting implementation is available under:

```text
Senarios/Improved/
```

The original implementation is preserved under:

```text
Senarios/Normal/
```

---

# 6. Performance Testing Scenarios

To ensure that the optimization was not only effective under one specific workload, the system was tested using two different traffic patterns.

### Continuous Load

Users are continuously added over time, progressively increasing the load on the system.

This scenario evaluates how the pipeline behaves as the number of concurrent users and request pressure continuously increase.

### Interval Load

Load is applied in separate intervals, with periods between them where the system can recover and return toward a stable state.

This scenario evaluates system behavior under repeated load and recovery cycles.

Both scenarios were executed for the **Normal** and **Improved** implementations.

This provides a more reliable comparison than relying on a single benchmark.

The collected metrics include:

* RPS
* Response Time
* Percentiles
* Number of Users
* Failure Rate
* Spark Input Rate
* Spark Process Rate
* Input Rows
* Batch Duration
* Operation Duration
* CPU Usage
* Memory Usage
* Network Traffic

Complete test results and screenshots are available under:

```text
Senarios/
├── Normal/
└── Improved/
```

---

# 7. Monitoring and Observability

The project includes monitoring at both the **application** and **infrastructure** levels.

### Application Monitoring

For each request, monitoring information is collected, including:

* Request ID
* Timestamp
* HTTP Status
* Processing Status
* Spark Latency
* Response Time
* Request Size

These events are published through Kafka and stored in ClickHouse for analysis.

### Infrastructure Monitoring

Prometheus, cAdvisor, and Node Exporter are used to monitor:

* CPU usage
* Memory usage
* Container resources
* Network traffic
* Host load
* Container status

### Spark Monitoring

Spark UI is used to analyze the Streaming Query and identify processing bottlenecks through metrics such as:

* Input Rate
* Process Rate
* Input Rows
* Batch Duration
* Operation Duration

### Kafka Monitoring

AKHQ can be used to inspect Kafka brokers, topics, and message flow.

---

# 8. Project Structure

The main repository structure is:

```text
.
├── Architecture/
│   ├── Base.png
│   └── Flows.png
│
├── Senarios/
│   ├── Normal/
│   │   ├── Continues/
│   │   └── Interval/
│   │
│   └── Improved/
│       ├── Continues/
│       └── Interval/
│
└── Source/
    ├── API/
    ├── Clickhouse/
    ├── Kafka/
    ├── Locust/
    ├── Monitoring/
    ├── Spark/
    ├── .env
    └── docker-compose.yml
```

---

# 9. Requirements

The main requirements are:

* Docker
* Docker Compose
* Git

Clone the repository and enter the `Source` directory:

```bash
cd Source
```

The main environment configuration is available in:

```text
Source/.env
```

---

# 10. Running the Project

The services should be started in dependency order rather than starting the entire stack simultaneously.

## Step 1 — Create the Docker Network

First, create the project network:

```bash
docker network create \
  --driver bridge \
  --subnet 172.25.0.0/24 \
  project_network
```

This network is shared by the project services.

---

## Step 2 — Start Kafka

Start the Kafka broker and controller first.

```bash
docker compose up -d kafka-controller kafka-broker
```

After Kafka has fully started, create the required topics.

First, give the initialization script executable permissions:

```bash
chmod 755 Kafka/initial/initial.sh
```

Then run:

```bash
./Kafka/initial/initial.sh
```

This creates the Kafka topics required by the application.

---

## Step 3 — Start the API

After Kafka is ready:

```bash
docker compose up -d api
```

FastAPI can now receive requests and publish them to Kafka.

---

## Step 4 — Start ClickHouse

Start ClickHouse:

```bash
docker compose up -d clickhouse
```

ClickHouse provides the reference data required by Spark and stores monitoring data.

---

## Step 5 — Start PySpark

After ClickHouse is available:

```bash
docker compose up -d pyspark
```

> **Important:** ClickHouse must be available before starting PySpark because Spark uses ClickHouse reference data during price calculation.

The Spark implementation and configuration are available under:

```text
Source/Spark/
```

---

## Step 6 — Start Locust

Once the complete processing pipeline is ready, Locust can be started:

```bash
docker compose up -d locust
```

Locust is used to execute the performance-testing scenarios.

---

# 11. Monitoring Stack

Monitoring services should also be started in dependency order.

### Step 1 — cAdvisor and Node Exporter

```bash
docker compose up -d cadvisor node_exporter
```

These services provide container and host-level metrics.

### Step 2 — Prometheus

```bash
docker compose up -d prometheus
```

Prometheus collects the available metrics.

### Step 3 — Grafana

```bash
docker compose up -d grafana
```

Grafana provides visualization and dashboards.

### Kafka Monitoring

AKHQ can also be started for Kafka inspection:

```bash
docker compose up -d akhq
```

---

# 12. Grafana Dashboards

The project includes prepared Grafana dashboards under:

```text
Source/Monitoring/Dashboards/
```

Before importing the dashboards, the required **Grafana Data Sources** must be configured.

After the Data Sources are available, the two JSON dashboard files can be imported into Grafana.

The dashboards provide visibility into metrics such as:

* API Response Time
* RPS
* Request Status
* Request Size
* Spark Processing
* CPU
* Memory
* Network Traffic
* Container Resources

---

# 13. Performance Results

The Improved implementation demonstrated a significant performance improvement compared with the original implementation.

The comparison was performed across both **Continuous** and **Interval** workloads rather than relying on a single test.

The improved implementation achieved:

* Higher sustainable request rates.
* Lower processing latency under comparable workloads.
* Improved Spark Process Rate.
* Reduced Batch Duration.
* Support for substantially higher concurrent-user levels.
* Low failure rates during the tested workloads.
* Stable CPU and memory behavior within the available system resources.

The performance analysis combines application-level results from Locust with Spark-level metrics and infrastructure-level monitoring.

This makes it possible to distinguish between an improvement in the API layer, the processing layer, and the underlying system resources.

Detailed benchmark results, graphs, Spark UI screenshots, and resource-monitoring screenshots are available in:

```text
Senarios/Normal/
Senarios/Improved/
```

---

# 14. Normal vs Improved

Both implementations are intentionally kept in the repository.

| Scenario     | Description                                                                      |
| ------------ | -------------------------------------------------------------------------------- |
| **Normal**   | Initial implementation used as the baseline for performance analysis             |
| **Improved** | Optimized implementation after identifying and addressing processing bottlenecks |

The key point is that the **core architecture remains the same in both versions**.

The optimization focused on improving the efficiency of the existing processing pipeline rather than replacing the architecture.

This makes the comparison useful for evaluating the actual impact of the optimization decisions.

---

# 15. Repository References

The repository contains all the implementation, architecture documentation, and test evidence required to reproduce and evaluate the project.

Key locations:

```text
Architecture/
```

System architecture diagrams.

```text
Source/
```

Complete implementation and Docker-based deployment.

```text
Senarios/Normal/
```

Baseline implementation and performance results.

```text
Senarios/Improved/
```

Optimized implementation and performance results.

```text
Source/Spark/
```

Spark Structured Streaming implementation and configuration.

```text
Source/API/
```

FastAPI implementation.

```text
Source/Kafka/
```

Kafka configuration and topic initialization.

```text
Source/Clickhouse/
```

ClickHouse configuration and data definitions.

```text
Source/Locust/
```

Load-testing implementation.

```text
Source/Monitoring/
```

Monitoring configuration and Grafana dashboards.

---

## Final Notes

This project demonstrates a complete **real-time, event-driven price calculation pipeline**, from HTTP request generation to asynchronous processing, result delivery, monitoring, and performance analysis.

The development process followed a practical engineering workflow:

**Design → Implement → Measure → Identify Bottlenecks → Optimize → Re-test**

The Normal scenario provides the baseline implementation and measurements, while the Improved scenario demonstrates how targeted optimization of the processing pipeline can significantly improve system performance while preserving the original architecture.

---