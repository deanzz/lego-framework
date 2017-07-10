package cn.jw.rms.data.framework.common.log

import java.io.{FileWriter, IOException}

import org.joda.time.DateTime

/**
  * Created by deanzhang on 2017/6/26.
  */
/**
  * 本地磁盘上的写日志接口
  * @param logDir 日志存放路径
  */
class LocalLogger(logDir: String) extends LoggerAPI {
  val logPath = s"$logDir/$logFileName"

  private def log(typ: String, content: String) = {
    val line = s"${DateTime.now().toString("yyyy-MM-dd HH:mm:ss")}_ ${typ}_ $content\n"
    println(line)
    fileAppend(logPath, line)
  }
  override def info(content: String): Unit = log("INFO", content)

  override def warn(content: String): Unit = log("WARN", content)

  override def error(content: String): Unit = log("ERROR", content)

  private def fileAppend(filePath: String, content: String) {
    var writer: FileWriter = null
    try {
      writer = new FileWriter(filePath, true)
      writer.write(content)
    } catch {
      case e: Exception => throw e
    } finally {
      if (writer != null) writer.close()
    }
  }

}
