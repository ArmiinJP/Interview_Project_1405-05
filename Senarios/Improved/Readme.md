# Improved Scenario

## Overview

The goal of this scenario was to **improve the performance of the existing system under high load without changing its core architecture**.

In the Normal scenario, the complete processing path—from receiving the request to stream processing and generating the response—was implemented and tested under different load patterns. The results showed that as the number of users increased, both response time and batch processing duration increased, eventually limiting the system's processing capacity.

Instead of immediately changing the architecture, the first step was to **identify the main bottleneck within the existing processing pipeline**.

---

## Bottleneck Investigation

Several components were considered as potential bottlenecks:

* API request handling and response time
* Load Generator and actual request generation rate
* Kafka and message ingestion rate
* Spark Structured Streaming and batch processing time
* Number and duration of Spark Jobs and Stages
* `Join` operations and intermediate data processing
* CPU and memory utilization
* Network I/O
* ClickHouse and the data access path

The Normal scenario showed that as the load increased, **Spark accounted for a significant portion of the processing time**. The increase in `Batch Duration` and `Operation Duration`, together with the narrowing gap between Input Rate and Process Rate, indicated that the Spark processing pipeline could become one of the main system limitations.

At the same time, system-level monitoring showed that PySpark was responsible for the highest CPU utilization during heavy processing. Therefore, before making architectural changes, the decision was made to **reduce the computational overhead inside the Spark pipeline**.

The corresponding Normal-scenario measurements are documented in:

* `Senarios/Normal/Continues/Pyspark_Result.png`
* `Senarios/Normal/Continues/System_Result.png`
* `Senarios/Normal/Interval/Pyspark_Result.png`
* `Senarios/Normal/Interval/System_Result.png`

---

## Optimization Approach

Once Spark was identified as one of the main areas for optimization, different parts of the processing pipeline were examined.

The approach was to make **small, targeted changes and evaluate their impact**, rather than redesigning the system.

The main focus was on:

1. Reducing intermediate data processing
2. Reducing expensive or unnecessary Spark operations
3. Reducing Join overhead
4. Keeping the same business logic and overall architecture

### 1. Removing `explode`

In the Normal implementation, `explode` was used to process the `products` array. This transformed each request into multiple rows based on the number of products, after which the data had to be aggregated again.

In the Improved implementation, the required calculations are performed directly on the `products` array using Spark SQL `aggregate`.

As a result, each request remains represented by a single row and the intermediate row expansion caused by `explode` is avoided.

This reduces:

* The number of intermediate rows
* Intermediate data processing
* Aggregation overhead
* Unnecessary processing caused by expanding and regrouping product data

### 2. Reducing Join Overhead

Another area examined was the enrichment stage.

Both `shipping` and `tax` information use the same `country, city` key. Therefore, the enrichment structure was adjusted so that the required information could be obtained with fewer Join operations.

For the small reference datasets, `broadcast` joins are used to avoid unnecessary large-scale shuffle operations.

The objective was to provide the same reference information while keeping the Spark processing path as lightweight as possible.

---

## Continuous Load

In the Continuous Load test, the number of users was continuously increased to observe system behavior under sustained load.

In the Normal scenario, failures started to appear at approximately **805 users**, indicating that the system was reaching its processing limit under this workload.

The Improved implementation was then tested under the same general workload pattern, reaching **3000 users**.

In addition to the increased capacity, Response Time was also reduced across the main percentiles:

| Metric |    Normal |  Improved |      Change |
| ------ | --------: | --------: | ----------: |
| p50    | 15,729 ms | 13,596 ms | **13.6% ↓** |
| p75    | 17,231 ms | 14,675 ms | **14.8% ↓** |
| p90    | 18,068 ms | 15,672 ms | **13.3% ↓** |
| p95    | 18,591 ms | 16,364 ms | **12.0% ↓** |
| p99    | 19,728 ms | 17,586 ms | **10.9% ↓** |
| Max    | 21,053 ms | 18,826 ms | **10.6% ↓** |

The improvement was therefore not limited to the median response time; a reduction was observed across the main response-time percentiles.

The complete Continuous Load results are documented in:

**Normal**

* `Senarios/Normal/Continues/Locust_Result_Server1.png`
* `Senarios/Normal/Continues/Locust_Result_Server2.png`
* `Senarios/Normal/Continues/Pyspark_Result.png`
* `Senarios/Normal/Continues/System_Result.png`

**Improved**

* `Senarios/Improved/Continues/Locust_Result_Server2.png`
* `Senarios/Improved/Continues/Pyspark_Result.png`
* `Senarios/Improved/Continues/System_Result.png`

---

## Interval Load

To verify that the improvement was not specific to Continuous Load, the system was also tested using an **Interval Load** pattern, where traffic was introduced through multiple load intervals and bursts.

In the Normal scenario, the test reached approximately **1000 users**, with the request rate remaining mostly within the range of approximately **20–100 RPS**.

In the Improved scenario, the number of users was increased to **5000**, with the request rate reaching approximately **200–250 RPS** during parts of the test.

The Improved test produced the following results:

| Metric         | Improved Interval |
| -------------- | ----------------: |
| Maximum Users  |         **5,000** |
| Total Requests |       **102,127** |
| Peak RPS       |      **≈250 RPS** |
| Success Rate   |          **≈99%** |
| p50            |     **11,135 ms** |
| p95            |     **15,426 ms** |
| p99            |     **17,089 ms** |
| Max            |     **19,628 ms** |

This result is important because it shows that the improvement was not limited to a specific workload pattern. The optimized pipeline was also able to handle substantially higher traffic under burst/interval workloads.

The complete Interval Load results are documented in:

**Normal**

* `Senarios/Normal/Interval/Locust_Result_Server1.png`
* `Senarios/Normal/Interval/Locust_Result_Server2.png`

**Improved**

* `Senarios/Improved/Interval/Locust_Result_Server2.png`
* `Senarios/Improved/Interval/Pyspark_Result.png`
* `Senarios/Improved/Interval/System_Result.png`

---

## Resource and Spark Analysis

Alongside the API-level metrics, Spark behavior and system resource utilization were monitored throughout the tests to determine whether the performance improvement was simply the result of increased resource consumption.

In the Improved scenario:

* PySpark CPU and memory usage remained within the allocated resource limits.
* Batch processing was able to handle a higher processing rate.
* The optimized pipeline required less intermediate processing.
* The relationship between `Input Rate`, `Process Rate`, and `Batch Duration` showed improved processing behavior under load.

The following metrics were specifically considered:

* `Input Rate`
* `Process Rate`
* `Input Rows`
* `Batch Duration`
* `Operation Duration`
* CPU Usage
* Memory Usage
* Network Traffic

The results indicate that the performance improvement was achieved primarily by **reducing the computational overhead of the Spark pipeline**, rather than simply allocating additional resources.

---

## Conclusion

The Improved scenario focused on optimizing the existing processing pipeline rather than redesigning the system.

The optimization process started by examining the different potential bottlenecks in the Normal scenario. After identifying Spark processing as one of the main areas limiting performance, the internal operations of the pipeline were analyzed.

The main changes were:

* Removing the unnecessary `explode` operation
* Performing product calculations directly with `aggregate`
* Reducing Join overhead for reference data
* Using `broadcast` for small lookup datasets

The results from both Continuous and Interval Load Tests demonstrate that these changes produced measurable improvements:

* **Continuous Load:** Response Time decreased by approximately **10–15%** across the main percentiles, while the test reached **3000 users**.
* **Interval Load:** The system reached **5000 users**, approximately **250 RPS** during parts of the test, and approximately **99% request success**.
* Spark and system-level monitoring were performed alongside both tests to validate the behavior of the optimized pipeline.

Overall, the Improved scenario demonstrates that the same core architecture used in the Normal scenario can achieve significantly better capacity and latency through **targeted optimization of the Spark processing pipeline**, without requiring additional system resources or a redesign of the overall architecture.
