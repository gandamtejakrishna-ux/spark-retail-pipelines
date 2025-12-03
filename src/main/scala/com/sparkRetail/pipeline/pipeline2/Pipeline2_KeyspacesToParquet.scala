package com.sparkRetail.pipeline.pipeline2

import com.sparkRetail.config.AppConfig
import org.apache.spark.sql.SparkSession

object Pipeline2_KeyspacesToParquet {
    def main(args: Array[String]): Unit = {

      // ------------------------------------------------------------
      // Spark Session (with Keyspaces config)
      // ------------------------------------------------------------
      val spark = SparkSession.builder()
        .appName("Pipeline2-Keyspaces-To-Parquet")
        .master("local[*]")

        // ---------- CASSANDRA (Amazon Keyspaces) ----------
        .config("spark.cassandra.connection.host", AppConfig.cassandra.host)
        .config("spark.cassandra.connection.port", AppConfig.cassandra.port)
        .config("spark.cassandra.connection.ssl.enabled", "true")
        .config("spark.cassandra.connection.ssl.trustStore.path",
          "/Users/dadmin/Documents/spark/spark-retail-pipelines/conf/truststore.jks")
        .config("spark.cassandra.connection.ssl.trustStore.password", "changeit")
        .config("spark.cassandra.auth.username", AppConfig.cassandra.username)
        .config("spark.cassandra.auth.password", AppConfig.cassandra.password)

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
      // Read from Amazon Keyspaces (Cassandra)
      // ------------------------------------------------------------
      val salesDF = spark.read
        .format("org.apache.spark.sql.cassandra")
        .option("keyspace", "cassandra_db")
        .option("table", "sales_data")
        .load()

      // ------------------------------------------------------------
      // Select required columns
      // ------------------------------------------------------------
      val selectedDF = salesDF.select(
        "customer_id",
        "order_id",
        "amount",
        "product_name",
        "quantity"
      )

      // ------------------------------------------------------------
      // Write partitioned Parquet to S3
      // ------------------------------------------------------------
      selectedDF.write
        .mode("overwrite")
        .partitionBy("customer_id")
        .parquet("s3a://spark-bucks/sales/parquet/")

      println("Pipeline 2 completed successfully — Parquet written to S3!")

      spark.stop()
    }

}
