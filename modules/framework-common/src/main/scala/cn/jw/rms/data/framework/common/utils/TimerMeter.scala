package cn.jw.rms.data.framework.common.utils

import org.slf4j.{Logger, LoggerFactory}
import cn.jw.rms.data.framework.common._

/**
  * Created by deanzhang on 15/12/31.
  */
object TimerMeter {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def start(jobName: String): Long = {
    val ns = System.nanoTime()
    logger.info(s"$MY_LOG_PRE Start [$jobName] ns = $ns")
    ns
  }

  def end(jobName: String, start: Long): String = {
    val ns = System.nanoTime()
    val s = (ns - start) / 1E9
    val rs = f"$s%1.3f"
    logger.info(s"$MY_LOG_PRE End [$jobName] ns = $ns, Elapsed Time = " + rs + "s")
    rs
  }

}
