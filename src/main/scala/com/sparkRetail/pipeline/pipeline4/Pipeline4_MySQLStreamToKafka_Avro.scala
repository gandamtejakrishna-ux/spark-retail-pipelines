package com.sparkRetail.pipeline.pipeline4

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.EncoderFactory

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._
import com.sparkRetail.config.AppConfig

import java.io.ByteArrayOutputStream

object Pipeline4_MySQLStreamToKafka_Avro {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Pipeline4-MySQL-To-Kafka-Avro")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // -------------------------
    // Load Avro schema
    // -------------------------
    val schemaString = """
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

    val schema = new Schema.Parser().parse(schemaString)

    // -------------------------
    // Kafka Producer config
    // -------------------------
    val props = new Properties()
    props.put("bootstrap.servers", AppConfig.kafka.bootstrap)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")

    val producer = new KafkaProducer[String, Array[Byte]](props)
    val topic = AppConfig.kafka.topic

    // -------------------------
    // MySQL Config
    // -------------------------
    val jdbcOptions = Map(
      "url" -> AppConfig.mysql.url,
      "user" -> AppConfig.mysql.user,
      "password" -> AppConfig.mysql.password,
      "driver" -> "com.mysql.cj.jdbc.Driver",
      "dbtable" -> "new_orders"
    )

    var lastSeenOrderId = 0

    println("🚀 Pipeline 4 started — polling MySQL and sending Avro to Kafka")

    while (true) {

      val df = spark.read
        .format("jdbc")
        .options(jdbcOptions)
        .load()
        .filter(col("order_id") > lastSeenOrderId)

      if (!df.isEmpty) {

        val rows = df.collect()
        println(s"📥 Found ${rows.length} new rows")

        lastSeenOrderId = df.agg(max("order_id")).as[Int].head()

        // -------------------------
        // Convert each row to Avro & send
        // -------------------------
        rows.foreach { row =>

          val record: GenericRecord = new GenericData.Record(schema)

          record.put("order_id", row.getAs[Int]("order_id"))
          record.put("customer_id", row.getAs[Int]("customer_id"))
          record.put("amount", row.getAs[Double]("amount"))
          record.put("created_at",
            row.getAs[java.sql.Timestamp]("created_at").toString
          )

          // Serialize to Avro binary

          // Serialize to Avro binary
          val out = new ByteArrayOutputStream()
          val writer = new GenericDatumWriter[GenericRecord](schema)
          val encoder = EncoderFactory.get().binaryEncoder(out, null)

          writer.write(record, encoder)
          encoder.flush()

          val avroBytes = out.toByteArray()

          out.close()

          // Send to Kafka
          val kafkaRecord =
            new ProducerRecord[String, Array[Byte]](
              topic,
              record.get("order_id").toString,
              avroBytes
            )

          producer.send(kafkaRecord)
          println(s"Sent to Kafka: $record")
        }
      }

      Thread.sleep(5000)
    }
  }
}
