package com.sundogsoftware.spark

import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MyFriendsByAge {

  case class Person(id:Int, name:String, age:Int, friends:Int)

  def main(args: Array[String]){

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("MyFriendsByAge")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val people = spark.read
      .option("header", "true")
      .option("inferSchema","true")
      .csv("data/fakefriends.csv")
      .as[Person]

    people.printSchema()

    val agePeople = people
      .select("age","friends")
      .groupBy("age")
      .agg(round(avg("friends"), scale = 2))
      .sort("age")
      .show()

    spark.stop()

  }

}
