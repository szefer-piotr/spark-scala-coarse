package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}


/** Find superheros with least connections */
object MyMostObscureSuperhero {

  case class SuperHeroNames(id: Int, name: String)
  case class SuperHero(value: String)

   def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create SparkSession using every core of the local machine
    val spark = SparkSession
      .builder()
      .appName("MyMostObscureSuperhero")
      .master("local[*]")
      .getOrCreate()

    val superHeroNamesSchema = new StructType()
      .add("id", IntegerType, nullable = true)
      .add("name", StringType, nullable = true)

    // Build up a hero ID -> name Dataset
    import spark.implicits._

    val names = spark.read
      .schema(superHeroNamesSchema)
      .option("sep", " ")
      .csv("data/Marvel-names.txt")
      .as[SuperHeroNames]

    val lines = spark.read
      .text("data/Marvel-graph.txt")
      .as[SuperHero]

    val connections = lines
      .withColumn("id", split(col("value"), pattern = " ")(0))
      .withColumn("connections", size(split(col("value"),pattern = " ")) - 1)
      .groupBy("id").agg(sum("connections").alias("connections"))

    val mostPopular = connections
      .filter($"connections" === 1)
      .sort($"connections".desc)
      .select("id")

    // Using the join operation
    val mostPopularJoined = mostPopular.joinWith(names, mostPopular("id") === names("id"))
      .alias("heroName")
      .show()

    spark.stop()

  }

}
