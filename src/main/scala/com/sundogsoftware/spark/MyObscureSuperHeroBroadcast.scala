package com.sundogsoftware.spark

import com.sundogsoftware.spark.MostPopularSuperheroDataset.SuperHero
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, size, split, sum, udf}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

import scala.io.{Codec, Source}

object MyObscureSuperHeroBroadcast {

  // I need to revise how the data are being read into the nameDict

  // So first of all create the class that has an integer for an ID and a string for a super hero name
  case class SuperHeroNames(id: Int, name: String)

  def main(args: Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.ERROR)

    // New schema... what is is for? To give names to columns?
    val superHeroNamesSchema = new StructType()
      .add("id", IntegerType, nullable = true)
      .add("name", StringType, nullable = true)

    // Create spark SESSION

    val spark = SparkSession
      .builder
      .appName("MyObscureSuperHeroBroadcast")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // It seems that I can read within the spark session
    val names = spark.read
      .schema(superHeroNamesSchema)
      .option("sep", " ")
      .csv("data/Marvel-names.txt")
      .as[SuperHeroNames]

    //val nameDict = names()

    // Load hero names
    //val nameDict = spark.sparkContext.broadcast(names)

    //println(nameDict)

    // Is there a need to create a schema here? Create a schema, then get the connections
    // and create a dataset with that schema
    // seems like it is not necessary!!!
    //val heroSchema = new StructType()
    // .add("heroID", IntegerType, nullable = true)
    // .add("connections", IntegerType, nullable = true)

    val lines = spark.read
      .text("data/Marvel-graph.txt")

    val connections = lines
      .withColumn("id", split(col("value"), " ")(0))
      .withColumn("connections", size(split(col("value"), " ")) - 1)
      .groupBy("id").agg(sum("connections").alias("connections"))

    val obscureHeroes = connections
      .filter($"connections" === 1)

    //val lookupName : Int => String = (heroID:Int) => {
    //  nameDict.value(heroID)
    //}

    //val lookupNameUDF = udf(lookupName)

    // Now just add names there
    //val heroesWithNames = obscureHeroes.withColumn("heroName", lookupNameUDF(col("id")))

    //heroesWithNames.show(truncate = false)

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