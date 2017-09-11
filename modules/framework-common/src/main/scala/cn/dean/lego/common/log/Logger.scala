package cn.dean.lego.common.log

import com.typesafe.config.Config
import scaldi.Injectable.inject
import scaldi.Injector

/**
  * Created by deanzhang on 2017/6/26.
  */
/**
  * 对外的日志调用接口
  * @param injector 注入器，用于获取配置中的日志存放路径，可是hdfs或local，hdfs路径必须以"hdfs://"开头
  */
class Logger(implicit injector: Injector) {
  private val conf = inject[Config]
  private val logDir = conf.getString("log.dir")
  private val logger = if(logDir.startsWith("hdfs://")) new HDFSLogger(logDir) else new LocalLogger(logDir)
  def info(content: String) = logger.info(content)
  def warn(content: String) = logger.warn(content)
  def error(content: String) = logger.error(content)
}
