package com.ads.helper

import java.text.SimpleDateFormat
import java.sql.Timestamp

class DateHelper {
  def ddmmyyyyToTimestamp(dateString: String): Timestamp = {
    val sourceFormat = new SimpleDateFormat("dd/MM/yyyy");
    new Timestamp(sourceFormat.parse(dateString).getTime)
  }
}