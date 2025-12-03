package com.sparkRetail.main

import com.sparkRetail.pipeline.pipeline1.Pipeline1_RdbmsToKeyspaces
import com.sparkRetail.pipeline.pipeline2.Pipeline2_KeyspacesToParquet
import com.sparkRetail.pipeline.pipeline3.Pipeline3_ParquetToJson
import com.sparkRetail.pipeline.pipeline4.Pipeline4_MySQLStreamToKafka_Avro
import com.sparkRetail.pipeline.pipeline5.Pipeline5_KafkaToS3

object Main {

  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      printMenu()
      sys.exit(1)
    }

    args(0) match {

      case "1" =>
        println("▶ Running Pipeline 1 — RDBMS → Amazon Keyspaces")
        Pipeline1_RdbmsToKeyspaces.main(Array())

      case "2" =>
        println("▶ Running Pipeline 2 — Keyspaces → S3 Parquet")
        Pipeline2_KeyspacesToParquet.main(Array())

      case "3" =>
        println("▶ Running Pipeline 3 — Parquet → JSON Aggregates → S3")
        Pipeline3_ParquetToJson.main(Array())

      case "4" =>
        println("▶ Running Pipeline 4 — MySQL Stream → Kafka (Avro)")
        Pipeline4_MySQLStreamToKafka_Avro.main(Array())

      case "5" =>
        println("▶ Running Pipeline 5 — Kafka Consumer → S3 JSON")
        Pipeline5_KafkaToS3.main(Array())

      case other =>
        println(s"Invalid option: $other")
        printMenu()
    }
  }

  def printMenu(): Unit = {
    println(
      """
        |================ SPARK PIPELINES ================
        |
        | Run using:
        |     sbt "run <number>"
        |
        | 1 → Pipeline 1 : RDBMS → Amazon Keyspaces
        | 2 → Pipeline 2 : Keyspaces → S3 Parquet
        | 3 → Pipeline 3 : Parquet → Aggregate → JSON → S3
        | 4 → Pipeline 4 : MySQL Structured Streaming → Kafka (Avro)
        | 5 → Pipeline 5 : Kafka Stream → JSON → S3
        |
        |=================================================
        |""".stripMargin)
  }
}
