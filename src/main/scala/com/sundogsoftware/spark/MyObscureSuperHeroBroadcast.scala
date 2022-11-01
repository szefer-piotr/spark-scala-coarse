package com.sundogsoftware.spark

import com.sundogsoftware.spark.MostPopularSuperheroDataset.SuperHero
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, size, split, sum, udf}
import org.apache.spark.sql.types.{IntegerType, StructType}

import scala.io.{Codec, Source}

object MyObscureSuperHeroBroadcast {

  //case class SuperHeroNames(id: Int, name: String)

  def loadHeroNames() : Map[Int, String] = {
    implicit val codec: Codec = Codec("ISO-8859-1")
    var heroNames:Map[Int, String] = Map()
    val lines = Source.fromFile("data/Marvel-names.txt")

    for (line <- lines.getLines()){
      val fields = line.split(" ")
      if (fields.length > 1){
        heroNames += (fields(0).toInt -> fields(1))
      }
    }

    lines.close()

    heroNames
  }

  /** Main function where the action happens */
  def main(args: Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("MyObscureSuperHeroBroadcast")
      .master("local[*]")
      .getOrCreate()

    // Load her names
    val nameDict = spark.sparkContext.broadcast(loadHeroNames())

    println(nameDict)

    // Is there a need to create a schema here? Create a schema, then get the connections
    // and create a dataset with that schema
    // seems like it is not necessary!!!
    val heroSchema = new StructType()
      .add("heroID", IntegerType, nullable = true)
      .add("connections", IntegerType, nullable = true)

    val lines = spark.read
      .text("data/Marvel-graph.txt")

    import spark.implicits._
    val connections = lines
      .withColumn("id", split(col("value"), " ")(0))
      .withColumn("connections", size(split(col("value"), " ")) - 1)
      .groupBy("id").agg(sum("connections").alias("connections"))

    val obscureHeroes = connections
      .filter($"connections" === 1)

    val lookupName : Int => String = (heroID:Int) => {
      nameDict.value(heroID)
    }

    val lookupNameUDF = udf(lookupName)

    // Now just add names there
    val heroesWithNames = obscureHeroes.withColumn("heroName", lookupNameUDF(col("id")))

    heroesWithNames.show(truncate = false)

    spark.stop()

  }

}


//def loadSuperheroNames (): Unit = {

  // Handle character encoding issues:
  //implicit val codec: Codec = Codec ("ISO-8859-1")

  // Create a map of Ints to Strings, and populate it from ...
  //var superheroNames: Map[Int, String] = Map ()

  //val maplines = Source.fromFile ("data/Marvel-names.txt")

  //for (line <- maplines.getLines () ) {
  //val fields = line.split (" ")
  //if (fields.length > 1) {
  //superheroNames += (fields (0).toInt -> fields (1) )
  //}
  //maplines.close ()

  //superheroNames
  //}

  //}


// Using UDF
// Scala Map of hero id to hero name

//val nameDict = spark.sparkContext.broadcast (loadSuperheroNames () )

///val lookupName: Int => String = (id: Int) => {
//  nameDict.value (id)
//  }

// val lookupNameUDF = udf (lookupName)
//
 // val heroesWithNames = mostPopular.withColumn ("heroName", lookupNameUDF (col ("id") ) )

 // val sortedHeroesWithNames = heroesWithNames.sort ("connections")

  //sortedHeroesWithNames.show (sortedHeroesWithNames.count.toInt, truncate = false)