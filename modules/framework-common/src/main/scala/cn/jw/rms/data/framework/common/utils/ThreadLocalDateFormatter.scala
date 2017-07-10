package cn.jw.rms.data.framework.common.utils

import java.text.{DateFormat, SimpleDateFormat}

/**
 * Created by dean on 10/10/15.
 */
object ThreadLocalDateFormatter {
  val dateFormatter = new ThreadLocal[DateFormat] {
    val formatStr = "yyyy-MM-dd"
    protected override def initialValue: DateFormat =
       new SimpleDateFormat(formatStr)
  }

  val dateTimeFormatter = new ThreadLocal[DateFormat] {
    val formatStr = "yyyy-MM-dd HH:mm:ss"
    protected override def initialValue: DateFormat =
      new SimpleDateFormat(formatStr)
  }
}
