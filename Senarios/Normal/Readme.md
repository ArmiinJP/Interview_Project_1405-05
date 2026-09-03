# Normal Scenario – Baseline Implementation

At this stage, the goal was to implement a complete, stable, and functional version of the real-time price calculation system, ensuring that the system could not only process requests correctly, but also provide monitoring, performance analysis, and load testing capabilities.

This scenario is considered the **Baseline Architecture**. It represents a fully implemented and operational version of the system that will be used as the reference point for further improvements in the following scenarios, with the goal of increasing capacity and reducing latency.

---

## Implemented Components

The implemented architecture consists of the following components:

- **FastAPI**
  - Receives price calculation requests
  - Generates a unique `request_id`
  - Sends requests to Kafka
  - Waits for the corresponding result and returns the response

- **Kafka**
  - `price_requests` for incoming requests
  - `price_results` for calculation results
  - `api_monitoring` for monitoring events

- **PySpark Structured Streaming**
  - Continuously consumes requests from Kafka
  - Loads reference data from ClickHouse
  - Calculates:
    - Product prices
    - Transportation fees
    - Taxes
    - Discounts
  - Produces the calculation result and sends it to Kafka

- **ClickHouse**
  - Stores reference tables:
    - `tax_rates`
    - `shipping_fees`
    - `promotions`
  - Stores monitoring logs:
    - `api_performance_logs`

- **Grafana + Prometheus**
  - System resource monitoring
  - API monitoring
  - Visualization of RPS, response time, and request status

- **Locust**
  - Generates load
  - Executes both interval-based and continuous load tests

---

## Monitoring

To analyze system performance, the following information is stored in ClickHouse:

- Response time
- Request status (`SUCCESS`, `TIMEOUT`)
- Request size
- Final price
- Request and user identifiers

These metrics make it possible to analyze the system's behavior under different load conditions.

Example monitoring dashboard:

- `Monitoring_Log_Result.png`

It is important to note that this dashboard represents the exact monitoring output of the **Continuous Load Test** described in the following section.

---

## Load Test Scenarios

Two different load testing approaches were performed.

### 1. Interval Load Test

In this test, the system load was increased gradually in stages:

**100 → 200 → 300 → ... → 1000 Users**

Each stage ran for approximately 5 minutes.

**Results:**

- The system remained stable under these conditions.
- No significant errors were observed.
- The architecture was able to handle moderate levels of load without major issues.

Test result:

- `Locust_Result_Server2_Interval.png`

---

### 2. Continuous Load Test

In this scenario:

- **Users = 2000**
- **Ramp Up = 5 Users/sec**

The load was continuously increased until reaching the configured number of users.

**Results:**

- RPS initially increased as the number of users increased.
- After the system reached its processing capacity, increasing the number of users no longer resulted in a corresponding increase in throughput.
- Response time gradually increased.
- A portion of the requests eventually failed because their processing time exceeded the configured timeout.

Test result:

- `Locust_Result_Server2_Continues.png`

---

## Spark Behavior

The behavior of Spark Structured Streaming showed that:

- The system remains stable under normal load.
- As the incoming workload increases, batch processing duration also increases.
- Eventually, Spark processing time exceeds the API timeout, causing requests to fail.

Spark monitoring result:

- `Pyspark_Result.png`

---

## Resource Utilization

During the load tests:

- Kafka showed relatively low resource consumption.
- ClickHouse remained stable.
- FastAPI introduced relatively low resource overhead.
- Spark was the dominant resource consumer and became the primary processing bottleneck under high load.

System monitoring result:

- `System_Result.png`

---

## Conclusion

This scenario represents a **complete and operational version of the system** that provides:

- Real-time processing
- An asynchronous message queue
- Monitoring capabilities
- Performance and analytical logging
- Load and capacity testing capabilities

The Normal Scenario therefore provides a reliable baseline for the next phase of the project.

The next phase will focus on improving the system's behavior under high traffic by increasing processing capacity and reducing latency, while keeping the same overall single-node architecture and comparing the results against this baseline.