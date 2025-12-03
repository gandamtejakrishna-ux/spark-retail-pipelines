package com.sparkRetail.config

import com.typesafe.config.{Config, ConfigFactory}

object AppConfig {
  val config: Config = ConfigFactory.load()

  object mysql {
    val url      = config.getString("mysql.url")
    val user     = config.getString("mysql.user")
    val password = config.getString("mysql.password")
  }

  object cassandra {
    val host     = config.getString("cassandra.host")
    val port     = config.getInt("cassandra.port")
    val username = config.getString("cassandra.username")
    val password = config.getString("cassandra.password")
    val keyspace = config.getString("cassandra.keyspace")
    val table    = config.getString("cassandra.table")
  }
  object aws {
    val accessKey=config.getString("aws.access_key")
    val secretKey=config.getString("aws.secret_key")
  }
  object kafka {
    val bootstrap=config.getString("kafka.bootstrap")
    val topic=config.getString("kafka.topic")
  }
}
