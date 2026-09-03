---

# Real-Time Price Calculation Platform

یک پلتفرم **Real-Time Price Calculation** برای پردازش درخواست‌های محاسبه قیمت در شرایط High Traffic که با استفاده از **FastAPI، Apache Kafka، Spark Structured Streaming، ClickHouse و Locust** پیاده‌سازی شده است.

هدف پروژه، طراحی و پیاده‌سازی یک معماری Event-Driven برای دریافت تعداد زیادی درخواست، پردازش غیرهمزمان آن‌ها و بازگرداندن نتیجه محاسبه قیمت با قابلیت مشاهده و تحلیل عملکرد سیستم است.

تمام اجزای اصلی پروژه به‌صورت Containerized و با استفاده از Docker Compose اجرا می‌شوند.

---

## 1. Architecture

معماری پروژه به‌صورت Single-Node طراحی شده و جریان اصلی به شکل زیر است:

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
   ├── Price Calculation
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

FastAPI مسئول دریافت درخواست، تولید `request_id` و مدیریت چرخه Request/Response است و منطق اصلی محاسبه قیمت در FastAPI انجام نمی‌شود.

Kafka به‌عنوان لایه ارتباطی و Buffer بین API و Spark عمل می‌کند و Spark Structured Streaming وظیفه پردازش Micro-Batchها و انجام محاسبات Business Logic را بر عهده دارد.

برای جزئیات معماری:

* [Architecture/Base.png](Architecture/Base.png) — نمای کلی معماری
* [Architecture/Flows.png](Architecture/Flows.png) — جریان‌های اصلی Request/Response، Reference Data و Monitoring

---

## 2. Main Components

| Component                      | Responsibility                                            |
| ------------------------------ | --------------------------------------------------------- |
| **Locust**                     | تولید بار و تست رفتار سیستم در شرایط مختلف                |
| **FastAPI**                    | دریافت HTTP Request، تولید `request_id` و مدیریت Response |
| **Kafka**                      | Buffer و ارتباط Async بین اجزای سیستم                     |
| **Spark Structured Streaming** | پردازش Micro-Batch و محاسبه قیمت                          |
| **ClickHouse**                 | نگهداری Reference Data و داده‌های Monitoring              |
| **Grafana**                    | نمایش Metrics و Performance                               |
| **Prometheus**                 | جمع‌آوری Metrics سیستم و Containerها                      |
| **cAdvisor**                   | Monitoring منابع Containerها                              |
| **Node Exporter**              | Monitoring منابع Host                                     |
| **AKHQ**                       | مشاهده و بررسی Kafka                                      |

جزئیات پیاده‌سازی هر سرویس در پوشه [Source](Source/) قرار دارد.

---

# 3. Price Calculation Flow

هر درخواست شامل اطلاعات سبد خرید، کشور، شهر و در صورت وجود کد تخفیف است.

Spark در هر Micro-Batch مراحل زیر را انجام می‌دهد:

1. محاسبه مجموع قیمت محصولات
2. محاسبه وزن کل محصولات
3. محاسبه هزینه حمل‌ونقل
4. محاسبه مالیات
5. محاسبه Discount
6. محاسبه Final Price
7. انتشار نتیجه در Kafka

Reference Data مورد استفاده Spark شامل:

* `tax_rates`
* `shipping_fees`
* `promotions`

است که از ClickHouse خوانده می‌شود.

---

# 4. Why Kafka + Spark?

در این پروژه، محاسبه قیمت عمداً از API جدا شده است.

API نباید با افزایش تعداد درخواست‌ها مستقیماً درگیر پردازش سنگین Business Logic شود. بنابراین Kafka نقش Buffer را ایفا کرده و Spark مسئول پردازش Stream است.

این طراحی باعث می‌شود:

* API سبک باقی بماند.
* Producer و Consumer از یکدیگر مستقل باشند.
* Traffic Spikeها توسط Kafka جذب شوند.
* پردازش بتواند به‌صورت Micro-Batch انجام شود.
* Performance هر بخش به‌صورت مستقل قابل بررسی باشد.

---

# 5. Performance Optimization

پس از پیاده‌سازی سناریوی اولیه، عملکرد سیستم تحت Load بررسی شد.

در سناریوی Normal، ابتدا رفتار کل Pipeline بررسی شد و سپس با استفاده از Metrics مربوط به:

* Locust
* Spark Structured Streaming
* CPU و Memory
* Network Traffic
* Kafka
* API
* Monitoring Logs

گلوگاه‌های احتمالی شناسایی شدند.

یکی از مهم‌ترین نقاط بررسی، **Spark Processing Layer** بود؛ زیرا با افزایش Load، زمان پردازش Micro-Batch و در نتیجه زمان رسیدن نتیجه به API اهمیت زیادی پیدا می‌کرد.

به‌جای تغییر معماری اصلی، Optimization به‌صورت مرحله‌ای انجام شد و بخش‌های مختلف Pipeline مورد بررسی قرار گرفتند.

در Spark مواردی مانند:

* تعداد و هزینه Joinها
* نحوه خواندن Reference Data
* استفاده از `broadcast`
* تعداد Partitionها
* Shuffle
* Parallelism
* ساختار Transformationها
* محاسبات داخل DataFrame
* هزینه پردازش هر Micro-Batch

بررسی و بهینه‌سازی شدند.

یکی از تغییرات مهم، کاهش هزینه Join مربوط به Reference Data بود. با توجه به کوچک بودن این Data و ماهیت Lookup آن، Joinها به شکلی انجام شدند که از ایجاد Shuffle غیرضروری جلوگیری شود.

همچنین تنظیمات Spark و ساختار Processing با هدف کاهش Batch Duration و افزایش Process Rate بازبینی شدند.

سناریوی بهبود‌یافته در مسیر زیر قرار دارد:

[Senarios/Improved](Senarios/Improved/)

و سناریوی اولیه برای مقایسه در مسیر زیر موجود است:

[Senarios/Normal](Senarios/Normal/)

---

# 6. Performance Scenarios

برای اینکه Optimization فقط در یک وضعیت خاص ارزیابی نشود، تست‌ها در دو الگوی مختلف انجام شدند:

### Continuous Load

در این حالت تعداد کاربران به‌صورت پیوسته افزایش پیدا می‌کند تا سیستم به Load بالاتری برسد.

### Interval Load

در این حالت Load به‌صورت بازه‌ای ایجاد می‌شود و بین هر بازه، سیستم فرصت بازگشت به وضعیت پایدار را دارد.

هر دو حالت برای **Normal** و **Improved** اجرا شده‌اند.

بنابراین امکان مقایسه Performance در چند وضعیت مختلف وجود دارد و نتایج تنها به یک تست منفرد وابسته نیستند.

نتایج تست‌ها شامل موارد زیر هستند:

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

نتایج کامل در پوشه [Senarios](Senarios/) قرار گرفته‌اند.

---

# 7. Monitoring

برای بررسی رفتار سیستم، Monitoring در چند سطح پیاده‌سازی شده است.

### Application Monitoring

اطلاعات مربوط به هر Request شامل مواردی مانند:

* Request ID
* Timestamp
* HTTP Status
* Processing Status
* Spark Latency
* Response Time
* Request Size

در سیستم Monitoring ثبت می‌شود.

### Infrastructure Monitoring

با استفاده از Prometheus، cAdvisor و Node Exporter موارد زیر قابل مشاهده هستند:

* CPU Usage
* Memory Usage
* Container Resource Usage
* Network Traffic
* Host Load
* Container Status

### Spark Monitoring

Spark UI برای بررسی مستقیم رفتار Streaming Query استفاده شده و Metrics مهمی مانند:

* Input Rate
* Process Rate
* Input Rows
* Batch Duration
* Operation Duration

برای تحلیل Bottleneckها بررسی شده‌اند.

### Kafka Monitoring

برای مشاهده وضعیت Kafka و Topicها می‌توان از AKHQ استفاده کرد.

---

# 8. Project Structure

ساختار اصلی پروژه:

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

پیش‌نیازهای اصلی:

* Docker
* Docker Compose
* Git

پس از Clone کردن Repository، وارد پوشه Source شوید:

```bash
cd Source
```

فایل `.env` برای تنظیمات محیطی پروژه در همین مسیر قرار دارد.

---

# 10. Running the Project

راه‌اندازی سرویس‌ها باید با توجه به وابستگی بین آن‌ها انجام شود.

> **نکته:** اجرای کل Stack به‌صورت همزمان توصیه نمی‌شود، زیرا برخی سرویس‌ها برای Startup صحیح به سرویس‌های دیگر وابسته هستند.

## Step 1 — Create Docker Network

ابتدا Network پروژه را ایجاد کنید:

```bash
docker network create \
  --driver bridge \
  --subnet 172.25.0.0/24 \
  project_network
```

این Network بین سرویس‌های پروژه استفاده می‌شود.

---

## Step 2 — Start Kafka

ابتدا Broker و Controller مربوط به Kafka را اجرا کنید:

```bash
docker compose up -d kafka-controller kafka-broker
```

پس از اینکه Kafka به‌طور کامل بالا آمد، Script ساخت Topicها را آماده و اجرا کنید.

ابتدا Permission لازم را بدهید:

```bash
chmod 755 Kafka/initial/initial.sh
```

سپس Script را اجرا کنید:

```bash
./Kafka/initial/initial.sh
```

این Script Topicهای مورد نیاز پروژه را ایجاد می‌کند.

Kafka در پروژه با Apache Kafka 4 اجرا شده و Broker نیز در Docker Compose دارای Healthcheck است. ([GitHub][2])

---

## Step 3 — Start API

پس از آماده شدن Kafka، API را اجرا کنید:

```bash
docker compose up -d api
```

API به Kafka وابسته است و در Compose نیز Startup آن به آماده بودن Broker وابسته شده است. ([GitHub][2])

---

## Step 4 — Start ClickHouse

سپس ClickHouse را اجرا کنید:

```bash
docker compose up -d clickhouse
```

ClickHouse شامل Reference Data مورد نیاز Spark و همچنین داده‌های Monitoring است. ([GitHub][2])

---

## Step 5 — Start PySpark

پس از بالا آمدن ClickHouse، سرویس PySpark را اجرا کنید:

```bash
docker compose up -d pyspark
```

> **مهم:** PySpark برای اجرای صحیح Calculation به Reference Data موجود در ClickHouse نیاز دارد؛ بنابراین ClickHouse باید قبل از Spark در دسترس باشد.

تنظیمات Spark، JDBC Driverها و Configurationهای مربوط به Streaming در مسیر [Source/Spark](Source/Spark/) قرار دارند. سرویس فعلی از Spark 4.0.1 استفاده می‌کند. ([GitHub][2])

---

## Step 6 — Start Locust

پس از آماده شدن کل Pipeline، Locust را می‌توان اجرا کرد:

```bash
docker compose up -d locust
```

Locust برای اجرای سناریوهای Load Testing استفاده می‌شود و می‌توان الگوهای مختلف Load مانند **Continuous** و **Interval** را بررسی کرد.

نتایج تست‌ها در:

[Senarios/Normal](Senarios/Normal/)

و

[Senarios/Improved](Senarios/Improved/)

قرار دارند.

---

# 11. Monitoring Stack

Monitoring یک Stack جداگانه دارد و برای اجرای آن بهتر است سرویس‌ها به ترتیب زیر راه‌اندازی شوند.

### Step 1 — cAdvisor & Node Exporter

ابتدا:

```bash
docker compose up -d cadvisor node_exporter
```

این سرویس‌ها Metrics مربوط به Container و Host را فراهم می‌کنند.

### Step 2 — Prometheus

پس از آن:

```bash
docker compose up -d prometheus
```

Prometheus Metrics سرویس‌های Monitoring را جمع‌آوری می‌کند.

### Step 3 — Grafana

در نهایت:

```bash
docker compose up -d grafana
```

Grafana برای Visualization و تحلیل Metrics استفاده می‌شود.

### Kafka Monitoring

برای مشاهده و بررسی Kafka نیز می‌توان AKHQ را اجرا کرد:

```bash
docker compose up -d akhq
```

---

# 12. Grafana Dashboards

داشبوردهای Grafana در Repository آماده شده‌اند.

قبل از Import کردن Dashboardها، ابتدا باید **Data Sourceهای مورد نیاز Grafana** ایجاد و تنظیم شوند.

پس از آماده شدن Data Sourceها، دو فایل JSON موجود در پوشه Dashboard را می‌توان در Grafana Import کرد.

این Dashboardها برای مشاهده مواردی مانند:

* API Response Time
* RPS
* Request Status
* Request Size
* Spark Processing
* CPU
* Memory
* Network
* Container Resources

استفاده می‌شوند.

---

# 13. Results

نتایج تست‌ها نشان دادند که Optimization انجام‌شده در Spark باعث بهبود قابل توجه رفتار سیستم تحت Load شده است.

در سناریوی **Improved**:

* RPS بالاتری قابل دستیابی بود.
* Response Time در Loadهای مشابه کاهش پیدا کرد.
* Spark Process Rate بهبود پیدا کرد.
* Batch Duration کاهش یافت.
* سیستم توانست تعداد کاربران بیشتری را مدیریت کند.
* CPU و Memory همچنان در محدوده قابل قبول باقی ماندند.
* Failure Rate در تست‌های انجام‌شده بسیار پایین باقی ماند.

نکته مهم این است که این نتایج فقط از یک تست به‌دست نیامده‌اند؛ Continuous و Interval Load هر دو بررسی شده‌اند و برای هر دو حالت، Metrics مربوط به Application، Spark و Infrastructure ثبت شده است.

نتایج تصویری کامل در پوشه [Senarios](Senarios/) قرار دارند.

---

# 14. Scenario Comparison

دو نسخه از سیستم برای مقایسه نگهداری شده‌اند:

| Scenario     | Description                                  |
| ------------ | -------------------------------------------- |
| **Normal**   | پیاده‌سازی اولیه و مبنای Performance Testing |
| **Improved** | نسخه بهینه‌شده پس از تحلیل Bottleneckها      |

در هر دو نسخه، **معماری اصلی سیستم ثابت باقی مانده است**؛ بهبودها عمدتاً با بازبینی نحوه پردازش، تنظیمات و هزینه عملیات داخلی انجام شده‌اند.

این ساختار امکان بررسی مستقیم اثر Optimizationها را بدون تغییر اساسی در معماری فراهم می‌کند.

---

# 15. Repository References

برای بررسی دقیق‌تر پروژه:

* [Architecture](Architecture/)
* [Source Code](Source/)
* [Normal Scenario](Senarios/Normal/)
* [Improved Scenario](Senarios/Improved/)
* [Spark Implementation](Source/Spark/)
* [API Implementation](Source/API/)
* [Kafka Configuration](Source/Kafka/)
* [ClickHouse Configuration](Source/Clickhouse/)
* [Locust Tests](Source/Locust/)
* [Monitoring](Source/Monitoring/)

---

## Final Note

این پروژه با تمرکز بر **Real-Time Processing، Event-Driven Architecture، Performance Testing و Performance Optimization** طراحی شده است.

فرآیند توسعه صرفاً به پیاده‌سازی اولیه محدود نشده و پس از ایجاد نسخه پایه، سیستم تحت Load واقعی بررسی شده، Bottleneckهای احتمالی شناسایی شده‌اند و سپس با تغییرات کنترل‌شده در لایه Processing، نسخه Improved ایجاد و مجدداً در چند الگوی مختلف Load ارزیابی شده است.

به این ترتیب Repository علاوه بر Source Code، مسیر طراحی، معماری، تست Performance، Monitoring و فرآیند Optimization را نیز مستند می‌کند.

[1]: https://github.com/ArmiinJP/Interview_Project_1405-05 "GitHub - ArmiinJP/Interview_Project_1405-05: Real-time price calculation platform built with FastAPI, Kafka, Spark Structured Streaming, and ClickHouse, with high-traffic performance testing and optimization. · GitHub"
[2]: https://github.com/ArmiinJP/Interview_Project_1405-05/blob/main/Source/docker-compose.yml "Interview_Project_1405-05/Source/docker-compose.yml at main · ArmiinJP/Interview_Project_1405-05 · GitHub"
