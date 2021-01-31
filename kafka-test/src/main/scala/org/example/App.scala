package org.example

import com.microsoft.azure.cosmosdb.spark.streaming.CosmosDBSinkProvider
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

//bootstrap server args(0) e.g. localhost:9092
//topic name args(1) e.g. kafka-poc
//winutils location only for windows args(2)

object App {
  def main(args: Array[String]): Unit = {

    println(System.getProperty("os.name"))
    if (System.getProperty("os.name").toLowerCase.startsWith("win")) {
      println("Inside if statement")
      System.setProperty("hadoop.home.dir", args(0))
    }

    val spark = SparkSession.builder().master("local[*]").getOrCreate()

    val writeConfig = Map(
      "Endpoint" -> "https://localhost:8081",
      "Masterkey" -> "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==",
      "Database" -> "SampleDB",
      "Collection" -> "Persons",
      "Upsert" -> "true",
      "WritingBatchSize" -> "500",
      "CheckpointLocation" -> "D://checkpointlocation_write1"
    )

    val df = spark.
      readStream.
      format("kafka").
      option("kafka.bootstrap.servers", "0.0.0.0:9092").
      option("subscribe", "test").
      option("startingOffsets", "earliest").
//      option("kafka.security.protocol", "SASL_SSL").
//      option("sasl.mechanism", "SCRAM-SHA-256").
//      option("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"kafkadev\" password=\"I)eVk@fk01\";").
//      option("ssl.truststore.location", "/etc/connect_ts/truststore.jks").
//      option("ssl.truststore.password", "<PASSWORD>").
//      option("ssl.keystore.location", "/etc/connect_ts/keystore.jks").
//      option("ssl.keystore.password", "<PASSWORD>").
//      option("ssl.key.password", "<PASSWORD>").
      load()

    val schema = (new StructType).add("firstName", StringType)
      .add("lastName", StringType)
      .add("id", StringType)

    val finaldf = df.select(from_json(col("value").cast("string"), schema) as "person")

//      finaldf.writeStream
//      .format("console")
//      .start()
//      .awaitTermination()
    finaldf.select("person")
      .writeStream
      .format(classOf[CosmosDBSinkProvider].getName)
      .options(writeConfig)
      .option("checkpointLocation","D:\\checkpoint3\\")
      .start()
      .awaitTermination()
  }
}
