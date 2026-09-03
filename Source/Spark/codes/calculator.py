import os
import traceback
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    aggregate, broadcast, col, coalesce, lit, struct, when,
    from_json
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, ArrayType
)

# Spark
REPARTITION_NUM = int(os.environ.get('REPARTITION_NUM'))
TRIGGER_TIME = os.environ.get('TRIGGER_TIME')
PATH_CHECKPOINT_SPARK = os.environ.get('PATH_CHECKPOINT_SPARK', None)
STARTING_OFFSET = os.environ.get('STARTING_OFFSET', None)

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "api_price_requests")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "api_price_results")

# Clickhouse
CLICKHOUSE_USER = os.getenv("CH_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CH_PASSWORD", "1234")
CLICKHOUSE_DB_NAME = os.getenv("CH_DB_NAME", "Base")
CLICKHOUSE_SERVER = os.getenv("CH_SERVER", "clickhouse:8123")

spark = (
    SparkSession.builder
    .appName("Price_Calculator")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

def load_reference_tables():
    shipping_tax = (
        spark.read
        .format("jdbc")
        .option("url", f"jdbc:clickhouse://{CLICKHOUSE_SERVER}/{CLICKHOUSE_DB_NAME}")
        .option(
            "query",
            """
            SELECT
                s.country,
                s.city,
                s.base_fee,
                s.per_kg_fee,
                t.tax_rate
            FROM Shipping AS s
            INNER JOIN Tax AS t
                ON s.country = t.country
                AND s.city = t.city
            """
        )
        .option("user", CLICKHOUSE_USER)
        .option("password", CLICKHOUSE_PASSWORD)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .load()
    )
    promotions = (
        spark.read
        .format("jdbc")
        .option("url", f"jdbc:clickhouse://{CLICKHOUSE_SERVER}/{CLICKHOUSE_DB_NAME}")
        .option("dbtable", "Promotions")
        .option("user", CLICKHOUSE_USER)
        .option("password", CLICKHOUSE_PASSWORD)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .load()
    )
    return (
        spark.sparkContext.broadcast(shipping_tax.collect()),
        spark.sparkContext.broadcast(promotions.collect())
    )

def process_batch(batch_df, batch_id):
    try:
        batch_df = transform(batch_df)
        write_to_kafka_batch(batch_df)
    except Exception as e:
        logging.error(e)
        traceback.print_exc()

def transform(df):
    calculated = (
        df
        .withColumn(
            "calculated",
            aggregate(
                "products",
                struct(
                    lit(0.0).alias("products_price"),
                    lit(0.0).alias("total_weight")
                ),
                lambda acc, x: struct(
                    (
                        acc["products_price"]
                        + x["quantity"] * x["price_per_unit"]
                    ).alias("products_price"),
                    (
                        acc["total_weight"]
                        + x["quantity"] * x["weight_per_unit"]
                    ).alias("total_weight")
                )
            )
        )
        .select(
            "request_id",
            "country",
            "city",
            "promo_code",
            col("calculated.products_price").alias("products_price"),
            col("calculated.total_weight").alias("total_weight")
        )
    )

    result = (
        calculated.join(
            broadcast(shipping_tax_df),
            ["country", "city"],
            "left"
        )
        .withColumn(
            "transportation_fee",
            coalesce(col("base_fee"), lit(0)) +
            coalesce(col("total_weight") * col("per_kg_fee"), lit(0))
        )
        .withColumn(
            "tax",
            (col("products_price") + col("transportation_fee")) *
            coalesce(col("tax_rate"), lit(0))
        )
    )
    result = (
        result.join(broadcast(promo_df), "promo_code", "left")
        .withColumn(
            "discount",
            when(
                col("discount_value").isNotNull(),
                (col("products_price") + col("transportation_fee")) *
                (col("discount_value") / 100)
            )
            .otherwise(lit(0))
        )
    )
    result = (
        result.withColumn(
            "final_price",
            col("products_price") + col("transportation_fee") + col("tax") - col("discount")
        )
        .select(
            "request_id",
            struct(
                col("products_price"),
                col("transportation_fee"),
                col("tax"),
                col("discount")
            ).alias("details"),
            col("final_price")
        )
        .withColumn("message", lit("Success"))
    )
    return result

def write_to_kafka_batch(df):
    (
        df.selectExpr("to_json(struct(*)) as value")
        .write
        .mode("default")
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", OUTPUT_TOPIC)
        .save()
    )

shipping_tax_bc, promo_bc = load_reference_tables()
shipping_tax_df = spark.createDataFrame(shipping_tax_bc.value)
promo_df = spark.createDataFrame(promo_bc.value)

product_schema = StructType([
    StructField("quantity", DoubleType()),
    StructField("price_per_unit", DoubleType()),
    StructField("weight_per_unit", DoubleType())
])
request_schema = StructType([
    StructField("request_id", StringType()),
    StructField("products", ArrayType(product_schema)),
    StructField(
        "destination",
        StructType([
            StructField("country", StringType()),
            StructField("city", StringType())
        ])
    ),
    StructField("promo_code", StringType())
])

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .option("minPartitions", str(REPARTITION_NUM))
    .load()
)
df = (
    df.selectExpr("CAST(value AS STRING) json")
    .select(from_json(col("json"), request_schema).alias("data"))
    .select(
        col("data.request_id").alias("request_id"),
        col("data.products").alias("products"),
        col("data.destination.country").alias("country"),
        col("data.destination.city").alias("city"),
        col("data.promo_code").alias("promo_code")
    )
)

while True:
    try:
        if PATH_CHECKPOINT_SPARK is not None:
            query = (
                df.writeStream
                .foreachBatch(process_batch)
                .outputMode("append")
                .trigger(processingTime=TRIGGER_TIME)
                .option("checkpointLocation", PATH_CHECKPOINT_SPARK)
                .start()
            )
            query.awaitTermination()
        else:
            query = (
                df.writeStream
                .foreachBatch(process_batch)
                .outputMode("append")
                .trigger(processingTime=TRIGGER_TIME)
                .start()
            )
            query.awaitTermination()
    except Exception as e:
        logging.error(f'-----------------------------------------------------------------------------------------------\nerror: {e}')
