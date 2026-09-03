import os
import json
import traceback
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, explode, sum as spark_sum, lit, coalesce, struct, to_json, when, from_json, broadcast)

from pyspark.sql.types import (StructType, StructField, StringType, DoubleType, ArrayType)


# Spark
REPARTITION_NUM = int(os.environ.get('REPARTITION_NUM'))
TRIGGER_TIME = os.environ.get('TRIGGER_TIME')
PATH_CHECKPOINT_SPARK = os.environ.get('PATH_CHECKPOINT_SPARK', None)
STARTING_OFFSET = os.environ.get('STARTING_OFFSET', None)
# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "api_price_requests")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "api_price_results")
# Clickhosue 
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
    tax_rates = (
        spark.read
        .format("jdbc")
        .option("url", f"jdbc:clickhouse://{CLICKHOUSE_SERVER}/{CLICKHOUSE_DB_NAME}")
        .option("dbtable", "Tax")
        .option("user", CLICKHOUSE_USER)
        .option("password", CLICKHOUSE_PASSWORD)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .load()
    )
    shipping_fees = (
        spark.read
        .format("jdbc")
        .option("url", f"jdbc:clickhouse://{CLICKHOUSE_SERVER}/{CLICKHOUSE_DB_NAME}")
        .option("dbtable", "Shipping")
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
        spark.sparkContext.broadcast(tax_rates.collect()),
        spark.sparkContext.broadcast(shipping_fees.collect()),
        spark.sparkContext.broadcast(promotions.collect())
    )

def process_batch(batch_df, batch_id):
    try:
        if batch_df.isEmpty():
            return
        batch_df = transform(batch_df)
        write_to_kafka_batch(batch_df)
    except Exception as e:
        logging.error(e)
        traceback.print_exc()

def transform(df):
    products = (
        df.withColumn("product", explode("products"))
        .select(
            "request_id",
            col("destination.country").alias("country"),
            col("destination.city").alias("city"),
            "promo_code",
            col("product.quantity"),
            col("product.price_per_unit"),
            col("product.weight_per_unit")
        )
    )

    calculated = (
        products
        .withColumn("product_price", col("quantity") * col("price_per_unit"))
        .withColumn("product_weight", col("quantity") * col("weight_per_unit"))
        .groupBy(
            "request_id", "country", "city", "promo_code"
        )
        .agg(
            spark_sum("product_price").alias("products_price"),
            spark_sum("product_weight").alias("total_weight")
        )
    )

    result = (
        calculated.join(broadcast(shipping_df), ["country", "city"], "left")
        .withColumn("transportation_fee", 
            coalesce(col("base_fee"), lit(0)) + coalesce(col("total_weight") * col("per_kg_fee"), lit(0)))
    )

    result = (
        result.join(broadcast(tax_df), ["country", "city"], "left")
        .withColumn("tax", 
            (col("products_price") + col("transportation_fee")) * coalesce(col("tax_rate"), lit(0))
        )
    )

    result = (
        result.join(broadcast(promo_df), "promo_code", "left")
        .withColumn("discount",
            when(col("discount_value").isNotNull(), (col("products_price") + col("transportation_fee")) * (col("discount_value")/100))
            .otherwise(lit(0))
        )
    )

    result = (
        result.withColumn("final_price",
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


tax_bc, shipping_bc, promo_bc = load_reference_tables()

tax_df = spark.createDataFrame(tax_bc.value)
shipping_df = spark.createDataFrame(shipping_bc.value)
promo_df = spark.createDataFrame(promo_bc.value)


# tax_df = spark.broadcast(tax_df)
# shipping_df = spark.broadcast(shipping_df)
# promo_df = spark.broadcast(promo_df)


product_schema = StructType([
    StructField("product_id", StringType()),
    StructField("quantity", DoubleType()),
    StructField("price_per_unit", DoubleType()),
    StructField("weight_per_unit", DoubleType())
])

request_schema = StructType([
    StructField("request_id", StringType()),
    StructField("user_id", StringType()),
    StructField("products", ArrayType(product_schema)),
    StructField("destination",
        StructType([
            StructField("country", StringType()),
            StructField("city", StringType()),
            StructField("postal_code", StringType())
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
    .select("data.*")
)


while True:
    try:
        if PATH_CHECKPOINT_SPARK is not None:
            query = (df.writeStream.foreachBatch(process_batch).outputMode("append").trigger(processingTime=TRIGGER_TIME).option("checkpointLocation",PATH_CHECKPOINT_SPARK).start())
            query.awaitTermination()
        else:
            query = (df.writeStream.foreachBatch(process_batch).outputMode("append").trigger(processingTime=TRIGGER_TIME).start())
            query.awaitTermination()
    except Exception as e:
        logging.error(f'-----------------------------------------------------------------------------------------------\nerror: {e}')

