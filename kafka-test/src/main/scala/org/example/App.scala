package org.example

import org.apache.spark.sql.SparkSession

//bootstrap server args(0) e.g. localhost:9092
//topic name args(1) e.g. kafka-poc
//winutils location only for windows args(2)

object App {
  def main(args: Array[String]): Unit = {

    println(System.getProperty("os.name"))
    if (System.getProperty("os.name").toLowerCase.startsWith("win")) {
      println("Inside if statement")
      System.setProperty("hadoop.home.dir", args(2))
    }

    val spark = SparkSession.builder().master("local[*]").getOrCreate()

    val df = spark.
      readStream.
      format("kafka").
      option("kafka.bootstrap.servers", args(0)).
      option("subscribe", args(1)).
      option("startingOffsets", "earliest").
      option("kafka.security.protocol", "SASL_SSL").
      option("sasl.mechanism", "SCRAM-SHA-256").
      option("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"kafkadev\" password=\"I)eVk@fk01\";").
      option("ssl.truststore.location", "/etc/connect_ts/truststore.jks").
      option("ssl.truststore.password", "<PASSWORD>").
      option("ssl.keystore.location", "/etc/connect_ts/keystore.jks").
      option("ssl.keystore.password", "<PASSWORD>").
      option("ssl.key.password", "<PASSWORD>").
      load()

    import spark.implicits._
    df.selectExpr("CAST(value AS STRING)")
        .as[(String)]

    df.writeStream
      .format("console")
      .start()
      .awaitTermination()
  }
}
