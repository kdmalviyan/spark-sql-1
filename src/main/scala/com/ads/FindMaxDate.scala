package com.ads

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FindMaxDate {
  def main(args: Array[String]) {

    val spark = SparkSession.builder
      .master("local")
      .appName("weighted rating for communication mode")
      .getOrCreate()

    val comCustMode = spark.read.option("header", "true").csv(args(0))

    comCustMode.createTempView("cumcustmode")

    val maxDate = spark.sql("SELECT cust_id,created_date FROM cumcustmode").groupBy("cust_id").agg(max("created_date") as "created_date")
    val allRec = spark.sql("SELECT cust_id,comm_mode_id,created_date FROM cumcustmode")
    allRec.join(maxDate,(maxDate.col("cust_id")===allRec.col("cust_id") && maxDate.col("created_date")===allRec.col("created_date"))).show()

  }

}