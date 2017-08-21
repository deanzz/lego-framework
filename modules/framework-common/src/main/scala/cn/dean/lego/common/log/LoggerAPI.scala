package cn.dean.lego.common.log

import org.joda.time.DateTime

/**
  * Created by deanzhang on 2017/6/26.
  */
/**
  * 日志调用类父接口
  */
trait LoggerAPI {

  def logFileName = DateTime.now.toString("yyyyMMddHHmmss")
  def info(content: String)
  def warn(content: String)
  def error(content: String)

}
