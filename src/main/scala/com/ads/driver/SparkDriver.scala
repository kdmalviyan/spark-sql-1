package com.ads.driver

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import com.ads.helper.Constants
import com.ads.helper.JoinHelper
import com.ads.entity.Order
import java.util.Date
import com.ads.helper.DateHelper
import com.ads.entity.Communication
import scala.collection.mutable.{ Map, HashMap }
import com.ads.entity.CommunicationType

object SparkDriver {
  def CONSTANTS = new Constants()
  def JOIN_HELPER = new JoinHelper()
  def DATE_HELPER = new DateHelper()

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Weighted rating for communication mode").setMaster("local")
    val sparkSession = SparkSession.builder().appName("Spalk SQL Example").config(conf).getOrCreate()
    import sparkSession.implicits._

    val orders = JOIN_HELPER.loadCSVAsDataFrame(sparkSession, CONSTANTS.orderCSV, true)
      .map(line => Order(line.get(0).toString().toInt, line.get(1).toString().toInt, line.get(2).toString().toInt,
        DATE_HELPER.ddmmyyyyToTimestamp(line.get(3).toString())))
    orders.createOrReplaceTempView("orders")

    val filteredOrders = sparkSession.sql("SELECT orderId, customerId, productId, createdDate FROM orders WHERE createdDate IN (SELECT MAX(createdDate) FROM orders GROUP BY customerId)").toDF()
    filteredOrders.createOrReplaceTempView("orders")

    val communications = JOIN_HELPER.loadCSVAsDataFrame(sparkSession, CONSTANTS.communicationCSV, true)
      .map(line => Communication(line.get(0).toString().toInt, line.get(1).toString().toInt, line.get(2).toString().toInt,
        DATE_HELPER.ddmmyyyyToTimestamp(line.get(3).toString())))
    communications.createOrReplaceTempView("communications")

    val comm_cust_date = sparkSession.sql("SELECT customerId, MAX(dateCreated) as dateCreated FROM communications GROUP BY customerId").toDF()
    comm_cust_date.createOrReplaceTempView("comm_cust_date")

    val filteredComm = sparkSession.sql("SELECT c.customerId, c.communicationModeId FROM communications c ,comm_cust_date ccd WHERE c.customerId = ccd.customerId AND c.dateCreated = ccd.dateCreated").toDF()
    filteredComm.createOrReplaceTempView("filteredComm")

    val commUseCount = sparkSession.sql("SELECT communicationModeId, count(*) as numOfTimes from filteredComm group by communicationModeId").toDF()
    commUseCount.createOrReplaceTempView("commUseCount")

    val commType = JOIN_HELPER.loadCSVAsDataFrame(sparkSession, CONSTANTS.communicationModeCSV, true)
      .map(line => CommunicationType(line.get(0).toString().toInt, line.get(1).toString()))
    commType.createOrReplaceTempView("commType")

    val commResult = sparkSession.sql("SELECT ct.commModeType, cc.numOfTimes  from commType ct, commUseCount cc where ct.communicationModeId = cc.communicationModeId")
    commResult.createOrReplaceTempView("commResult")
    val result = sparkSession.sql("SELECT commModeType, numOfTimes from commResult WHERE numOfTimes IN (SELECT MAX(numOfTimes) from commResult)").toDF()
    val bestCommType = result.select("commModeType").toString()
    println(result.select("commModeType").collect().head + " is most effective mode of addvertisement, It is used by " + result.select("numOfTimes").collect().head)

    sparkSession.stop()
  }
}

