from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, struct, max_by
from pyspark.sql.types import *
from delta.tables import DeltaTable

BRONZE_PATH = "s3a://lakehouse/bronze/abp_orders"
SILVER_PATH = "s3a://lakehouse/silver/orders"

spark = SparkSession.builder.appName("abp-orders-bronze-to-silver").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

envelope_schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("Id", LongType())
        ])),
        StructField("after", StructType([
            StructField("Id", LongType()),
            StructField("OrderNumber", StringType()),
            StructField("StoreId", LongType()),
            StructField("CustomerId", StringType()),
            StructField("OrderTotal", StringType()),
            StructField("CreationTime", LongType()),
            StructField("OrderStatus", IntegerType()),
            StructField("IsDeleted", BooleanType())
        ])),
        StructField("op", StringType()),
        StructField("ts_ms", LongType())
    ]))
])

bronze_df = spark.read.format("delta").load(BRONZE_PATH)

parsed_df = bronze_df.select(
    from_json(col("kafka_value"), envelope_schema).alias("e")
)

valid_df = parsed_df.select(
    col("e.payload.after.*"),
    col("e.payload.op"),
    col("e.payload.ts_ms")
).filter(col("Id").isNotNull())

dedup_df = valid_df.groupBy("Id").agg(
    max_by(struct("*"), col("ts_ms")).alias("r")
).select("r.*")

if not DeltaTable.isDeltaTable(spark, SILVER_PATH):
    dedup_df.write.format("delta") \
        .mode("overwrite") \
        .save(SILVER_PATH)
else:
    DeltaTable.forPath(spark, SILVER_PATH) \
        .alias("t") \
        .merge(dedup_df.alias("s"), "t.Id = s.Id") \
        .whenMatchedDelete("s.op = 'd'") \
        .whenMatchedUpdateAll("s.op != 'd'") \
        .whenNotMatchedInsertAll("s.op != 'd'") \
        .execute()

spark.stop()
