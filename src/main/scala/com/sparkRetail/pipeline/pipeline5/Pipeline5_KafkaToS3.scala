package com.sparkRetail.pipeline.pipeline5

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.avro.functions.from_avro
import com.sparkRetail.config.AppConfig

object Pipeline5_KafkaToS3 {

  def main(args: Array[String]): Unit = {

    // --------------------------------------------
    // Spark Session + S3 Config
    // --------------------------------------------
    val spark = SparkSession.builder()
      .appName("Pipeline5-Kafka-Avro-To-S3-JSON")
      .master("local[*]")
      .config("spark.hadoop.fs.s3a.access.key", AppConfig.aws.accessKey)
      .config("spark.hadoop.fs.s3a.secret.key", AppConfig.aws.secretKey)
      .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-southeast-2.amazonaws.com")
      .config("spark.hadoop.fs.s3a.region", "ap-southeast-2")
      .config("spark.hadoop.fs.s3a.path.style.access", "false")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      .getOrCreate()

    import spark.implicits._

    // --------------------------------------------
    // Avro Schema used in Pipeline 4
    // --------------------------------------------
    val avroSchema =
      """
      {
        "type": "record",
        "name": "OrderRecord",
        "namespace": "com.retail",
        "fields": [
          { "name": "order_id", "type": "int" },
          { "name": "customer_id", "type": "int" },
          { "name": "amount", "type": "double" },
          { "name": "created_at", "type": "string" }
        ]
      }
      """

    // --------------------------------------------
    // 1. Read Avro bytes from Kafka topic
    // --------------------------------------------
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", AppConfig.kafka.bootstrap)
      .option("subscribe", "orders_avro_topic")
      .option("startingOffsets", "earliest")
      .load()

    // --------------------------------------------
    // 2. Decode Avro -> DataFrame columns
    // --------------------------------------------
    val decodedDF = kafkaDF
      .select(from_avro(col("value"), avroSchema).alias("data"))
      .select("data.*")

    // --------------------------------------------
    // 3. Convert DF rows → JSON string
    // --------------------------------------------
    val jsonDF = decodedDF
      .select(to_json(struct($"*")).alias("value"))

    // --------------------------------------------
    // 4. Write JSON to S3
    // --------------------------------------------
    val query = jsonDF.writeStream
      .format("json")
      .option("path", "s3a://spark-bucks/stream/json/")
      .option("checkpointLocation", "/tmp/pipeline5-checkpoint/")
      .outputMode("append")
      .start()

    println("🚀 Pipeline 5 started → Kafka Avro → JSON → S3")

    query.awaitTermination()
  }
}
