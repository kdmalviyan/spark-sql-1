package com.ads

import org.apache.spark.sql.SparkSession

object WeightedAds {
  def main(args: Array[String]) {

    val spark = SparkSession.builder
      .master("local")
      .appName("weighted rating for communication mode")
      .getOrCreate()

    val orderDF = spark.read.option("header", "true").csv(args(0))
    val customerDF = spark.read.option("header", "true").csv(args(1))

    orderDF.createTempView("orders")
    customerDF.createTempView("customer")

    val records = spark.sql("select distinct o.order_id,o.cust_id,c.cust_name,o.created_date from orders o,customer c where o.cust_id=c.cust_id")
    records.show()

  }
}