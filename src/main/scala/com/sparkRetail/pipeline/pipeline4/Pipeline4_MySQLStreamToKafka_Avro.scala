package com.sparkRetail.pipeline.pipeline4

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.avro.functions.to_avro
import com.sparkRetail.config.AppConfig

object Pipeline4_MySQLStreamToKafka_Avro {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Pipeline4-MySQL-Streaming-To-Kafka-Avro")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Avro schema
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


    // JDBC options
    val jdbcOptions = Map(
      "url" -> AppConfig.mysql.url,
      "user" -> AppConfig.mysql.user,
      "password" -> AppConfig.mysql.password,
      "driver" -> "com.mysql.cj.jdbc.Driver",
      "dbtable" -> "new_orders"
    )

    // Track max order_id seen so far
    var lastSeenOrderId = 0

    println("🚀 Pipeline 4 running — polling MySQL every 5 sec")

    while (true) {

      // ---- 1. Read MySQL table (batch) ----
      val batchDF = spark.read
        .format("jdbc")
        .options(jdbcOptions)
        .load()
        .filter(col("order_id") > lastSeenOrderId)

      if (!batchDF.isEmpty) {

        println("📥 New records found in MySQL!")

        // Update last seen
        lastSeenOrderId = batchDF.agg(max("order_id")).as[Int].head()

        // ---- 2. Convert to Avro ----
//        val avroDF = batchDF.select(
//          to_avro(struct(
//            col("order_id"),
//            col("customer_id"),
//            col("amount"),
//            col("created_at").cast("string")
//          )).as("value")
//        )
        val avroStruct = struct(
          col("order_id").cast("int").alias("order_id"),
          col("customer_id").cast("int").alias("customer_id"),
          col("amount").cast("double").alias("amount"),
          date_format(col("created_at"), "yyyy-MM-dd HH:mm:ss").alias("created_at")
        )
        val avroDF = batchDF.select(
          to_avro(avroStruct, avroSchema).alias("value")
        )


        // ---- 3. Write to Kafka ----
        avroDF.write
          .format("kafka")
          .option("kafka.bootstrap.servers", AppConfig.kafka.bootstrap)
          .option("topic", AppConfig.kafka.topic)
          .save()

        println("Sent to Kafka successfully!")
      }

      Thread.sleep(5000)
    }
  }
}

