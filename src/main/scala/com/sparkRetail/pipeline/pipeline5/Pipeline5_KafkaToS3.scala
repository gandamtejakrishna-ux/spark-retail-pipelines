//package com.sparkRetail.pipeline.pipeline5
//
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.avro.functions.from_avro
//import com.sparkRetail.config.AppConfig
//
//object Pipeline5_KafkaToS3 {
//
//  def main(args: Array[String]): Unit = {
//
//    val spark = SparkSession.builder()
//      .appName("Pipeline5-Kafka-Avro-To-S3-JSON")
//      .master("local[*]")
//      .config("spark.hadoop.fs.s3a.access.key", AppConfig.aws.accessKey)
//      .config("spark.hadoop.fs.s3a.secret.key", AppConfig.aws.secretKey)
//      .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-southeast-2.amazonaws.com")
//      .config("spark.hadoop.fs.s3a.region", "ap-southeast-2")
//      .config("spark.hadoop.fs.s3a.path.style.access", "false")
//      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
//      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
//      .config("spark.hadoop.fs.s3a.aws.credentials.provider",
//        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
//      .getOrCreate()
//
//    spark.sparkContext.setLogLevel("WARN")
//
//    // ------------------------------------------------
//    // Avro schema (MUST MATCH pipeline 4)
//    // ------------------------------------------------
//    val avroSchema =
//      """
//      {
//        "type": "record",
//        "name": "OrderRecord",
//        "namespace": "com.retail",
//        "fields": [
//          { "name": "order_id", "type": "int" },
//          { "name": "customer_id", "type": "int" },
//          { "name": "amount", "type": "double" },
//          { "name": "created_at", "type": "string" }
//        ]
//      }
//      """
//
//    // ------------------------------------------------
//    // 1. Read Avro bytes from Kafka
//    // ------------------------------------------------
//    val kafkaDF = spark.readStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", AppConfig.kafka.bootstrap)
//      .option("subscribe", "orders_avro_topic")
//      .option("startingOffsets", "earliest")
//      .load()
//
//    // ------------------------------------------------
//    // 2. Decode Avro → columns
//    // ------------------------------------------------
//    val decodedDF = kafkaDF
//      .select(from_avro(col("value"), avroSchema).alias("data"))
//      .select("data.*")
//
//    // ------------------------------------------------
//    // Debug: Print decoded output to console
//    // ------------------------------------------------
//    val debugQuery = decodedDF.writeStream
//      .format("console")
//      .outputMode("append")
//      .option("truncate", false)
//      .start()
//
//    // ------------------------------------------------
//    // 3. Convert each row to JSON string
//    // ------------------------------------------------
//    val jsonDF = decodedDF.select(
//      to_json(struct(decodedDF.columns.map(col): _*)).alias("value")
//    )
//
//    // ------------------------------------------------
//    // 4. Write JSON to S3
//    // ------------------------------------------------
//    val s3Query = jsonDF.writeStream
//      .format("json")
//      .option("path", "s3a://spark-bucks/stream/json/")
//      .option("checkpointLocation", "s3a://spark-bucks/checkpoints/pipeline5/")
//      .outputMode("append")
//      .start()
//
//    println("🚀 Pipeline 5 started → Kafka → Avro Decode → JSON → S3")
//
//    s3Query.awaitTermination()
//  }
//}
package com.sparkRetail.pipeline.pipeline5

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.avro.functions._
import com.sparkRetail.config.AppConfig

/**
 * Pipeline 5: Kafka Consumer to S3 JSON
 *
 * This pipeline:
 * 1. Reads Avro messages from Kafka topic 'orders_avro_topic'
 * 2. Decodes Avro messages into columns
 * 3. Converts output to JSON format
 * 4. Writes JSON files to S3: s3://retail-output/stream/json/
 */
object Pipeline5_KafkaToS3 {

  def main(args: Array[String]): Unit = {

    // -----------------------------
    // Spark Session with S3 Configuration
    // -----------------------------
    val spark = SparkSession.builder()
      .appName("Pipeline5-Kafka-Avro-To-S3-JSON")
      .master("local[*]")
      // S3 Configuration
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

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    // -----------------------------
    // Kafka Configuration
    // -----------------------------
    val kafkaBootstrap = AppConfig.kafka.bootstrap  // "localhost:9093"
    val kafkaTopic = AppConfig.kafka.topic          // "orders_avro_topic"

    // -----------------------------
    // S3 Output Paths
    // -----------------------------
    val s3OutputPath = "s3a://spark-bucks/stream/json/"
    val checkpointPath = "s3a://spark-bucks/checkpoints/pipeline5/"

    // -----------------------------
    // Avro Schema (MUST MATCH Pipeline 4 EXACTLY)
    // -----------------------------
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

    // -----------------------------
    // 1. Read from Kafka - START FROM LATEST (skip old data)
    // -----------------------------
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrap)
      .option("subscribe", kafkaTopic)
      .option("startingOffsets", "latest")  // ← Changed to "latest" to skip old data
      .option("failOnDataLoss", "false")    // ← Don't fail if data is lost
      .load()

    println(s"✓ Connected to Kafka: $kafkaTopic (reading from LATEST offset)")

    // -----------------------------
    // 2. Decode Avro → columns with error handling
    // Use PERMISSIVE mode to handle malformed records
    // -----------------------------
    val decodedDF = kafkaDF
      .select(
        col("key").cast("string").as("kafka_key"),
        col("timestamp").as("kafka_timestamp"),
        from_avro(col("value"), avroSchema).alias("data")
      )
      .select(
        col("kafka_key"),
        col("kafka_timestamp"),
        col("data.*")
      )
      .filter(col("order_id").isNotNull)  // Filter out malformed records

    println("✓ Avro decoding configured with error handling")

    // Add processing metadata
    val enrichedDF = decodedDF
      .withColumn("processing_time", current_timestamp())
      .withColumn("date_partition", date_format(current_timestamp(), "yyyy-MM-dd"))

    // -----------------------------
    // 3. Debug: Print decoded output to console
    // -----------------------------
    val debugQuery = enrichedDF.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", false)
      .start()

    println("✓ Console debug stream started")

    // -----------------------------
    // 4. Write JSON to S3
    // -----------------------------
    val s3Query = enrichedDF.writeStream
      .format("json")
      .option("path", s3OutputPath)
      .option("checkpointLocation", checkpointPath)
      .outputMode("append")
      .partitionBy("date_partition")
      .start()

    println("✓ S3 write stream started")

    // -----------------------------
    // Pipeline Status
    // -----------------------------
    println("\n" + "="*60)
    println("🚀 Pipeline 5 - Kafka to S3 (JSON)")
    println("="*60)
    println(s"📥 Kafka Topic     : $kafkaTopic")
    println(s"🔌 Kafka Bootstrap : $kafkaBootstrap")
    println(s"📤 S3 Output Path  : $s3OutputPath")
    println(s"💾 Checkpoint Path : $checkpointPath")
    println(s"📝 Output Format   : JSON")
    println(s"🗂  Partitioned By  : date_partition")
    println(s"🖥  Console Debug   : Enabled")
    println(s"⚠️  Starting Offset : LATEST (skips old data)")
    println("="*60)
    println("Press Ctrl+C to stop\n")

    // Wait for termination
    s3Query.awaitTermination()
  }
}
