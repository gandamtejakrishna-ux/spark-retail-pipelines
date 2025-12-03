package com.sparkRetail.pipeline.pipeline3

import com.sparkRetail.config.AppConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Pipeline3_ParquetToJson {

  def main(args: Array[String]): Unit = {

    // ------------------------------------------------------------
    // Spark Session
    // ------------------------------------------------------------
    val spark = SparkSession.builder()
      .appName("Pipeline3-Parquet-To-JSON")
      .master("local[*]")

      // ---------- S3 CONFIG ----------
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

    // ------------------------------------------------------------
    // 1. Read Parquet from S3 (Pipeline 2 output)
    // ------------------------------------------------------------
    val inputPath = "s3a://spark-bucks/sales/parquet/"

    val parquetDF = spark.read.parquet(inputPath)

    // ------------------------------------------------------------
    // 2. Aggregation: total quantity + total revenue per product
    // ------------------------------------------------------------
    val aggregatedDF = parquetDF
      .groupBy("product_name")
      .agg(
        sum("quantity").alias("total_quantity"),
        sum("amount").alias("total_revenue")
      )

    // ------------------------------------------------------------
    // 3. Write output as JSON to S3
    // ------------------------------------------------------------
    aggregatedDF.write
      .mode("overwrite")
      .json("s3a://spark-bucks/aggregates/products.json")

    println("Pipeline 3 completed — Aggregated JSON written to S3!")

    spark.stop()
  }
}
