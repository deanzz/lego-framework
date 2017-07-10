package cn.jw.rms.data.framework.common.log

/**
  * Created by deanzhang on 2017/6/26.
  */
/**
  * 对外的日志调用接口
  * @param logDir 日志存放路径，可是hdfs或local，hdfs路径必须以"hdfs://"开头
  */
case class Logger(logDir: String) {
  private val logger = if(logDir.startsWith("hdfs://")) new HDFSLogger(logDir) else new LocalLogger(logDir)
  def info(content: String) = logger.info(content)
  def warn(content: String) = logger.warn(content)
  def error(content: String) = logger.error(content)
}
