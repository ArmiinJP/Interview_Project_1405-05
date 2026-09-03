
---

# Improved Scenario


هدف این سناریو، **بهبود عملکرد سیستم موجود تحت بار بالا** بدون تغییر در معماری اصلی آن بود.

در سناریوی Normal، ابتدا معماری و مسیر کامل پردازش از دریافت درخواست تا پردازش Stream و تولید پاسخ بررسی و تحت بارهای مختلف تست شد. نتایج نشان دادند که با افزایش تعداد کاربران، زمان پاسخ و مدت پردازش Batch افزایش پیدا می‌کند و در بارهای بالاتر، ظرفیت پردازش سیستم محدود می‌شود.

در این مرحله، به جای اینکه بلافاصله معماری را تغییر دهیم، ابتدا تلاش شد **گلوگاه اصلی در مسیر موجود شناسایی شود**.

---

## Bottleneck Investigation

برای پیدا کردن علت محدودیت عملکرد، چند بخش به عنوان مظنون اصلی بررسی شدند:

* API و زمان دریافت و ارسال درخواست‌ها
* Load Generator و نرخ واقعی تولید درخواست
* Kafka و نرخ ورود داده به Stream
* Spark Structured Streaming و زمان پردازش Batch
* تعداد Job و Stageهای ایجادشده توسط Spark
* عملیات‌های `Join` و پردازش داده‌های میانی
* مصرف CPU و Memory
* Network I/O
* ClickHouse و مسیر ذخیره‌سازی

در بررسی نتایج Normal، مشخص شد که با افزایش بار، **Spark بخش قابل توجهی از زمان پردازش هر Batch را به خود اختصاص می‌دهد**. در داشبورد Spark، افزایش `Batch Duration` و `Operation Duration` و همچنین نزدیک شدن Process Rate به Input Rate نشان می‌داد که بخش پردازشی Pipeline می‌تواند یکی از محدودیت‌های اصلی سیستم باشد.

همزمان، داشبورد منابع سیستم نیز نشان داد که PySpark در زمان پردازش بار اصلی، بیشترین مصرف CPU را دارد؛ بنابراین تصمیم گرفته شد قبل از تغییر معماری، **هزینه‌ی داخلی Pipeline Spark کاهش داده شود**.

تصاویر مربوط به این بررسی در نتایج سناریوی Normal قرار دارند:

* `Senarios/Normal/Continues/Pyspark_Result.png`
* `Senarios/Normal/Continues/System_Result.png`
* `Senarios/Normal/Interval/Pyspark_Result.png`
* `Senarios/Normal/Interval/System_Result.png`

---

## Optimization Approach

پس از مشخص شدن Spark به عنوان یکی از مهم‌ترین نقاط قابل بهبود، چند مورد مختلف در Pipeline بررسی شد و تصمیم گرفته شد تغییرات **مرحله‌به‌مرحله و با کمترین تغییر در معماری** انجام شوند.

تمرکز اصلی روی دو موضوع بود:

1. کاهش حجم پردازش‌های میانی
2. کاهش تعداد عملیات‌های سنگین و مراحل اجرای Spark

### 1. Removing `explode`

در نسخه‌ی Normal، برای پردازش `products` از `explode` استفاده می‌شد. این عملیات باعث می‌شد هر Request بر اساس تعداد محصولات خود به چندین Row تبدیل شود و سپس برای محاسبه‌ی مقادیر مورد نیاز، داده‌ها مجدداً aggregate شوند.

در نسخه‌ی Improved، این محاسبات مستقیماً روی آرایه‌ی `products` با استفاده از Spark SQL `aggregate` انجام شدند.

در نتیجه، برای هر Request همچنان یک Row حفظ می‌شود و نیازی به ایجاد و پردازش Rowهای میانی مربوط به `explode` وجود ندارد.

این تغییر باعث کاهش:

* تعداد Rowهای میانی
* حجم پردازش
* عملیات aggregation
* و overhead مربوط به پردازش داده‌های میانی

شد.

### 2. Reducing Join Overhead

بخش دیگری که بررسی شد، Joinهای مورد استفاده برای enrichment بود.

اطلاعات `shipping` و `tax` هر دو بر اساس کلید مشترک `country, city` استفاده می‌شدند. بنابراین ساختار enrichment بررسی شد تا تعداد Joinهای غیرضروری کاهش پیدا کند.

برای Lookupهای کوچک نیز از `broadcast` استفاده شد تا Spark مجبور به انجام Shuffle گسترده برای این داده‌ها نباشد.

هدف این بخش این بود که همان اطلاعات مورد نیاز، با **کمترین هزینه‌ی ممکن در مسیر پردازش Spark** در دسترس باشند.

---

## Continuous Load

در Continuous Load، تعداد کاربران به صورت پیوسته افزایش داده شد تا رفتار سیستم در بار بالا بررسی شود.

در سناریوی Normal، با افزایش بار، از حدود **805 کاربر** به بعد خطاها شروع شدند و ظرفیت سیستم در این محدوده تحت تأثیر قرار گرفت.

در سناریوی Improved، همان معماری پس از بهینه‌سازی Pipeline مجدداً تحت بار قرار گرفت و تست تا **3000 کاربر** ادامه پیدا کرد.

در تست Improved، علاوه بر افزایش ظرفیت، Response Time نیز در تمام percentileهای اصلی کاهش پیدا کرد:

| Metric |    Normal |  Improved |      Change |
| ------ | --------: | --------: | ----------: |
| p50    | 15,729 ms | 13,596 ms | **13.6% ↓** |
| p75    | 17,231 ms | 14,675 ms | **14.8% ↓** |
| p90    | 18,068 ms | 15,672 ms | **13.3% ↓** |
| p95    | 18,591 ms | 16,364 ms | **12.0% ↓** |
| p99    | 19,728 ms | 17,586 ms | **10.9% ↓** |
| Max    | 21,053 ms | 18,826 ms | **10.6% ↓** |

بنابراین بهبود فقط در median مشاهده نشد و در کل توزیع Response Time نیز کاهش قابل مشاهده‌ای ایجاد شد.

نتایج مربوط به Load Test، Spark و منابع سیستم در تصاویر زیر مستند شده‌اند:

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

برای اینکه مشخص شود بهبود فقط در Continuous Load اتفاق نیفتاده است، سیستم در شرایط **Interval Load** نیز آزمایش شد؛ یعنی بار به صورت دوره‌ای و در چند Burst مختلف وارد سیستم شد.

در سناریوی Normal، تست با حدود **1000 کاربر** انجام شد و نرخ درخواست در بخش عمده‌ی تست در محدوده‌ی حدود **20 تا 100 RPS** قرار داشت.

در سناریوی Improved، تعداد کاربران تا **5000 کاربر** افزایش داده شد و در بخش‌هایی از تست نرخ پردازش به حدود **200–250 RPS** رسید.

در این تست:

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

این نتیجه اهمیت دارد، زیرا نشان می‌دهد بهینه‌سازی انجام‌شده وابسته به یک الگوی خاص از بار نیست و در workloadهای burst و interval نیز ظرفیت پردازش سیستم به شکل محسوسی افزایش پیدا کرده است.

نتایج کامل تست Interval در تصاویر زیر قرار دارند:

**Normal**

* `Senarios/Normal/Interval/Locust_Result_Server1.png`
* `Senarios/Normal/Interval/Locust_Result_Server2.png`

**Improved**

* `Senarios/Improved/Interval/Locust_Result_Server2.png`
* `Senarios/Improved/Interval/Pyspark_Result.png`
* `Senarios/Improved/Interval/System_Result.png`

---

## Resource and Spark Analysis

در کنار معیارهای مربوط به API، منابع سیستم و رفتار داخلی Spark نیز در طول تست‌ها بررسی شدند تا مشخص شود افزایش performance ناشی از مصرف غیرعادی منابع نیست.

در تست Improved، مصرف CPU و Memory مربوط به PySpark در محدوده‌ی قابل کنترل باقی ماند و در عین حال Batchها توانستند نرخ بالاتری از داده را پردازش کنند.

همچنین بررسی Spark نشان داد که Pipeline بهینه‌شده با تعداد و هزینه‌ی پردازشی کمتری نسبت به حالت قبل اجرا می‌شود.

این موضوع در مقایسه‌ی موارد زیر قابل مشاهده است:

* `Input Rate`
* `Process Rate`
* `Input Rows`
* `Batch Duration`
* `Operation Duration`
* CPU Usage
* Memory Usage
* Network Traffic

بنابراین optimization صرفاً با افزایش منابع سیستم انجام نشده است؛ بلکه **هزینه‌ی پردازش خود Pipeline نیز کاهش پیدا کرده است**.

---

## Conclusion

در این مرحله، معماری اصلی سیستم بدون تغییر باقی ماند و تمرکز روی بهینه‌سازی مسیر پردازش موجود قرار گرفت.

فرآیند بهبود از بررسی bottleneckهای مختلف شروع شد و پس از مشخص شدن Spark به عنوان یکی از نقاط اصلی قابل بهبود، عملیات داخل Pipeline بررسی شدند. نتیجه‌ی این بررسی، حذف پردازش غیرضروری ناشی از `explode`، انجام محاسبات آرایه‌ای با `aggregate` و کاهش overhead مربوط به Joinهای enrichment بود.

نتایج دو نوع Load Test نیز نشان دادند که این تغییرات صرفاً یک بهبود تئوری نیستند:

* در **Continuous Load**، Response Time در تمام percentileهای اصلی حدود **10–15٪ کاهش** پیدا کرد و تست تا **3000 کاربر** ادامه یافت.
* در **Interval Load**، سیستم تا **5000 کاربر** و حدود **250 RPS** در بخش‌هایی از تست پیش رفت و حدود **99٪ درخواست‌ها موفق** بودند.
* در هر دو حالت، رفتار Spark و منابع سیستم نیز همزمان بررسی شد تا اثر optimization از افزایش صرف منابع تفکیک شود.

در نتیجه، سناریوی Improved در واقع **همان معماری و مسیر اصلی سناریوی Normal را با Pipeline پردازشی بهینه‌تر** اجرا می‌کند و نشان می‌دهد که قبل از تغییرات معماری، کاهش هزینه‌ی عملیات داخلی سیستم نیز می‌تواند تأثیر قابل توجهی بر ظرفیت و latency داشته باشد.
