package com.ads.entity

import java.sql.Timestamp

case class Communication(id: Int, customerId: Int, communicationModeId: Int, dateCreated: Timestamp) {
}