package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._

object MyTotalAmount {

  def parseLine(line: String): (Int, Float) = {
    // Read the line
    val fields = line.split(",")
    val customerId = fields(0).toInt
    val amount = fields(2).toFloat
    (customerId, amount)
  }

  def main(args: Array[String]) {

    // Set the log level to only print ERRORS
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "TotalAmount")

    val input = sc.textFile("data/customer-orders.csv")

    val customerDataset = input.map(parseLine)

    val customerAmounts = customerDataset.reduceByKey( (x,y) => (x + y))

    val customerSortedAmounts = customerAmounts.sortByKey()

    val results = customerSortedAmounts.collect()

    results.foreach(println)
  }

}