package com.test.scala

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Max Annual Price map reduce example
  */
object App {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Annual Max Price")
    val spark = SparkSession.builder.config(conf).getOrCreate()

    val csvFileName: String = args(0)
    val outputFileName: String = args(1)

    val dataFrame = spark.read.format("CSV")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(csvFileName)

    println(dataFrame.printSchema)

    val dataRDD = dataFrame.rdd

    // year -> max price
    val annualMaxPrice = dataRDD
        .filter(r => !r.isNullAt(0) && !r.isNullAt(1)
          && !r(0).equals("null") && !r(1).equals("null"))
        .map(r => (r(0).toString.split("-")(0).toInt, r(1).toString.toFloat))
        .reduceByKey((a, b) =>  Math.max(a, b ))
        .sortBy(_._1)

    annualMaxPrice.saveAsTextFile(outputFileName)
  }
}
