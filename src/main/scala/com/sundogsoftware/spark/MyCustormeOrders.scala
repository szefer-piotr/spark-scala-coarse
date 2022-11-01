package com.sundogsoftware.spark

import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType}
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MyCustormeOrders {

  case class Customer(cust_id:Int, product_id: Int, amount: Float)

  def main(args: Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("CustomerOrders")
      .master("local[*]")
      .getOrCreate()

    // Declare Schema
    val customerSchema = new StructType()
      .add("cust_id", IntegerType, nullable = true)
      .add("product_id", IntegerType, nullable = true)
      .add("amount", FloatType, nullable = true)

    //Read the file and construct dataset with hard coded compile structure (Schema)
    import spark.implicits._
    val ds = spark.read
      .schema(customerSchema)
      .csv("data/customer-orders.csv")
      .as[Customer]

    // Manipulations

    val totalSpentByCustomer = ds.groupBy("cust_id")
      .sum("amount")
      .sort("sum(amount)")

    val totSpentFormatted = totalSpentByCustomer
      .withColumn("Total", round($"sum(amount)", scale = 2))
      .select("cust_id","Total")
      .show()

    //val results = totSpentFormatted.collect()

    //results.foreach(println)

    spark.stop()

  }

}
