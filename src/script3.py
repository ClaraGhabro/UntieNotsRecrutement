import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars spark-sql-kafka-0-10_2.11-2.4.5.jar,kafka-clients-2.4.1.jar pyspark-shell'
from pyspark.sql import SparkSession


Q2_PATH = "../output/q2"
Q3_PATH = "../output/q3"


def consume_from_kafka(spark, topic):
    return spark .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "localhost:9092") \
      .option("subscribe", topic) \
      .option("startingOffsets", "earliest") \
      .load()


def save_to_parquet(queue, path):
    return queue \
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("checkpointLocation", path) \
        .option("path", path) \
        .start()


if __name__ == "__main__":
    spark = SparkSession.builder.appName("save_to_parquet").getOrCreate()
    df_queue2 = consume_from_kafka(spark, "topicKeyword")
    query_queue2 = save_to_parquet(df_queue2, Q2_PATH)

    df_queue3 = consume_from_kafka(spark, "topicName")
    query_queue3 = save_to_parquet(df_queue3, Q3_PATH)

    query_queue2.awaitTermination()
    query_queue3.awaitTermination()
