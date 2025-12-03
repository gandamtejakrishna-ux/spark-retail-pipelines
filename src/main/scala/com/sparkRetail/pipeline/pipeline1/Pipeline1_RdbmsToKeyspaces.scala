package com.sparkRetail.pipeline.pipeline1

import com.sparkRetail.config.AppConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

object Pipeline1_RdbmsToKeyspaces {

  def main(args: Array[String]): Unit = {

    // ----------------------------------------------------------------------
    // Spark Session with Cassandra (Amazon Keyspaces) Configuration
    // ----------------------------------------------------------------------
    val spark = SparkSession.builder()
      .appName("Pipeline1-RDBMS-To-Keyspaces")

      .master("local[*]")   // <-- ADD THIS
      .config("spark.cassandra.connection.host", AppConfig.cassandra.host)
      .config("spark.cassandra.connection.port", AppConfig.cassandra.port)
      .config("spark.cassandra.connection.ssl.trustStore.path",
        "/Users/dadmin/Documents/spark/spark-retail-pipelines/conf/truststore.jks")
      .config("spark.cassandra.connection.ssl.trustStore.password", "changeit")
      .config("spark.cassandra.connection.ssl.enabled", "true")
      .config("spark.cassandra.auth.username", AppConfig.cassandra.username)
      .config("spark.cassandra.auth.password", AppConfig.cassandra.password)
      .getOrCreate()

    // ----------------------------------------------------------------------
    // MySQL JDBC Configuration
    // ----------------------------------------------------------------------
    val jdbcUrl  = AppConfig.mysql.url
    val mysqlUser = AppConfig.mysql.user
    val mysqlPass = AppConfig.mysql.password

    def readTable(table: String): DataFrame = {
      spark.read
        .format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable", table)
        .option("user", mysqlUser)
        .option("password", mysqlPass)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .load()
    }

    // ----------------------------------------------------------------------
    // Read the 3 MySQL tables
    // ----------------------------------------------------------------------
    val customersDF   = readTable("customers")
    val ordersDF      = readTable("orders")
    val orderItemsDF  = readTable("order_items")

    // ----------------------------------------------------------------------
    // Join logic
    // ----------------------------------------------------------------------
    val customerOrdersDF = customersDF.join(
      ordersDF,
      customersDF("customer_id") === ordersDF("customer_id")
    )

    val finalDF = customerOrdersDF.join(
        orderItemsDF,
        customerOrdersDF("order_id") === orderItemsDF("order_id")
      )
      .select(
        customersDF("customer_id"),
        customersDF("name"),
        customersDF("email"),
        customersDF("city"),
        ordersDF("order_id"),
        ordersDF("order_date"),
        ordersDF("amount"),
        orderItemsDF("item_id"),
        orderItemsDF("product_name"),
        orderItemsDF("quantity")
      )

    // ----------------------------------------------------------------------
    // Write to Amazon Keyspaces (Cassandra)
    // ----------------------------------------------------------------------
    finalDF.write
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", AppConfig.cassandra.keyspace)
      .option("table", AppConfig.cassandra.table)
      .mode("append")
      .save()

    println("Pipeline 1 completed successfully — Data written to Amazon Keyspaces!")

    spark.stop()
  }
}
