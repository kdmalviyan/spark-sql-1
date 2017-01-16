package com.ads.entity

import java.sql.Timestamp

case class Order(orderId: Int, customerId: Int, productId: Int, createdDate: Timestamp) {

}