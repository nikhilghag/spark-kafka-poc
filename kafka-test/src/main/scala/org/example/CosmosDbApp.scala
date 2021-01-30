package org.example

import com.microsoft.azure.cosmosdb.spark.config.Config
import com.microsoft.azure.cosmosdb.spark.streaming.CosmosDBSourceProvider
import org.apache.spark.sql.SparkSession

object CosmosDbApp {
  def main(args: Array[String]): Unit = {

    println(System.getProperty("os.name"))
    if (System.getProperty("os.name").toLowerCase.startsWith("win")) {
      println("Inside if statement")
      System.setProperty("hadoop.home.dir", args(0))
    }

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    // Read Configuration
//    val readConfig = Config(Map(
//      "Endpoint" -> "https://localhost:8081",
//      "Masterkey" -> "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==",
//      "Database" -> "SampleDB",
//      "Collection" -> "Persons",
//      "InferStreamSchema" -> "true",
//      "ChangeFeedCheckpointLocation" -> "D:\\tmp"
//    ))

    val readConfig = Map(
          "Endpoint" -> "https://localhost:8081",
          "Masterkey" -> "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==",
          "Database" -> "SampleDB",
          "Collection" -> "Persons",
          "InferStreamSchema" -> "true",
          "ChangeFeedQueryName" -> "Departure-Delays",
          "ChangeFeedStartFromTheBeginning" -> "true"
        )
    // Connect via azure-cosmosdb-spark to create Spark DataFrame
//    val df = spark.read.cosmosDB(readConfig)
//    df.show()

    val df = spark.readStream.format(classOf[CosmosDBSourceProvider].getName).options(readConfig).load()
    df.writeStream
      .format("console")
      .start()
      .awaitTermination()
  }
}
