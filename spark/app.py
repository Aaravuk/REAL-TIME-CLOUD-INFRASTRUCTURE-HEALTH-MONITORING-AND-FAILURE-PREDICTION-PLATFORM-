from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    when,
    to_json,
    struct,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)

spark = (
    SparkSession.builder
    .appName("failure-detection-stream")
    .getOrCreate()
)

schema = StructType([
    StructField("server_id", StringType(), True),
    StructField("cpu", IntegerType(), True),
    StructField("memory", IntegerType(), True),
    StructField("disk", IntegerType(), True),
    StructField("response_time", IntegerType(), True),
    StructField("error_count", IntegerType(), True),
])

df = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        "kafka-kafka-bootstrap.kafka.svc.cluster.local:9092"
    )
    .option("subscribe", "system-logs")
    .option("startingOffsets", "latest")
    .load()
)

parsed = (
    df.selectExpr("CAST(value AS STRING) as json")
      .select(from_json(col("json"), schema).alias("data"))
      .select("data.*")
)

scored = (
    parsed
    .withColumn(
        "failure_score",
        when(col("cpu") > 90, 25).otherwise(0)
        + when(col("memory") > 90, 25).otherwise(0)
        + when(col("response_time") > 2000, 25).otherwise(0)
        + when(col("error_count") > 10, 25).otherwise(0)
    )
    .withColumn(
        "status",
        when(col("failure_score") >= 50, "FAILURE_LIKELY")
        .otherwise("HEALTHY")
    )
)

prediction_stream = (
    scored.select(
        to_json(
            struct(
                col("server_id"),
                col("cpu"),
                col("memory"),
                col("disk"),
                col("response_time"),
                col("error_count"),
                col("failure_score"),
                col("status")
            )
        ).alias("value")
    )
)

console_query = (
    scored.writeStream
    .format("console")
    .outputMode("append")
    .start()
)

kafka_query = (
    prediction_stream.writeStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        "kafka-kafka-bootstrap.kafka.svc.cluster.local:9092"
    )
    .option("topic", "failure-predictions")
    .option(
        "checkpointLocation",
        "/tmp/checkpoints/failure-predictions"
    )
    .outputMode("append")
    .start()
)

print("Kafka sink started")

spark.streams.awaitAnyTermination()
