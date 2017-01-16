package com.ads.helper

import org.apache.spark.sql._
import org.apache.spark.annotation.InterfaceStability

class JoinHelper {
  def CONSTANTS = new Constants()
  def JOIN_HELPER = new JoinHelper()
  def DATE_HELPER = new DateHelper()
  
  def joinExample(spark: SparkSession, company: DataFrame, product: DataFrame): Unit = {
    val product = loadCSVAsDataFrame(spark, CONSTANTS.productCSV, true)
    product.createOrReplaceTempView("product")
    val company = loadCSVAsDataFrame(spark, CONSTANTS.companyCSV, true)
    company.createOrReplaceTempView("company")
    val crossProduct = product.crossJoin(company)
    val join = spark.sql("SELECT c.company_name as COMPANY_NAME, p.product_name as PRODUCT_NAME from company c, product p where p.company_id = c.company_id")
    displayRecords(join)
  }

  def loadCSVAsDataFrame(spark: SparkSession, path: String, header: Boolean): DataFrame = {
    spark.read.option("header", header)
      .csv(path)
  }

  def displayRecords(df: DataFrame): Unit = {
    df.show()
  }
}