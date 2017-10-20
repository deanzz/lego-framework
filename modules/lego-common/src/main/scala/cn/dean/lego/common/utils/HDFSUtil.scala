package cn.dean.lego.common.utils

import java.io._
import java.net.URI
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import scala.util.{Failure, Success, Try}

/**
  * Created by deanzhang on 16/1/20.
  */
object HDFSUtil {
  val hadoopConf = new org.apache.hadoop.conf.Configuration()

  def delete(hdfsHost: String, output: String, isRecusrive: Boolean = true) = {
    val hdfs = FileSystem.get(new URI(hdfsHost), hadoopConf)
    hdfs.delete(new Path(output), isRecusrive)
  }

  def checkFileExists(hdfsHost: String, output: String) = {
    val hdfs = FileSystem.get(new URI(hdfsHost), hadoopConf)
    hdfs.exists(new Path(output))
  }

  def readFile(hdfsHost: String, path: String) = {
    val hdfs = FileSystem.get(new java.net.URI(hdfsHost), hadoopConf)
    val sb = new StringBuilder
    val in = hdfs.open(new Path(path))
    val br = new BufferedReader(new InputStreamReader(in))
    var line = br.readLine
    while (line != null) {
      try {
        line = br.readLine
        sb.append(line).append("\n")
      }
    }
    sb.toString()
  }

  def createNewFile(path: String) = {
    val hdfs = FileSystem.get(new URI(path), hadoopConf)
    hdfs.createNewFile(new Path(path))
  }

  //@tailrec
  def appendFile(path: String, content: String): Boolean = {
    /*var bfWriter: BufferedWriter = null
    var writer: OutputStreamWriter = null
    var out: FSDataOutputStream = null
    try {
      hadoopConf.setBoolean("dfs.support.append", true)
      val hdfs = FileSystem.get(new URI(path), hadoopConf)
      out = hdfs.append(new Path(path))
      writer = new OutputStreamWriter(out)
      bfWriter = new BufferedWriter(writer)
      bfWriter.write(content)
      true
    } catch {
      case _: FileNotFoundException =>
        createNewFile(path)
        appendFile(path, content)
      case e: Exception => throw e
    } finally {
      if (null != bfWriter) bfWriter.close()
      if (null != writer) writer.close()
      if (null != out) out.close()
    }*/
    true
  }

  /**
    * 获取HDFS中一个目录下的文件或目录列表，用来支持getMaxDt方法
    * @param path 待查看的hdfs目录，必须以hdfs://开头
    * @return 目录列表的数组
    */
  def listChildren(path: String) = {
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(path), hadoopConf)
    hdfs.listStatus(new org.apache.hadoop.fs.Path(path))
  }

  /**
    * 获取HDFS中一个目录下最大的日期分区的日期数字，此方法只支持日期或日期小时中，每部分均占位2位的形式，
    * 支持日期或小时举例：
    * 20170706
    * 2017-07-06
    * 2017/07/06
    * 2017070616（分时目录）
    * 2017-07-06_16
    * ...
    * 不支持的日期或小时举例：
    * 201776
    * 2017076
    * 2017706
    * 201707064
    * ...
    * @param path 待查看的hdfs目录，必须以hdfs://开头
    * @param childNodeNameRegex 待查找的子目录名的正则表达式，日期部分必须被括号扩住，以便正则表达式提取日期
    *                           比如：
    *                           dt=20170706 => dt=(\d{8})
    *                           20170706 => (\d{8})
    *                           2017070610 => (\d{10})
    *                           2017-07-06 => (\d{4}-\d{2}-\d{2})
    * @return 返回日期数字，格式为yyyyMMdd
    */
  def getMaxDt(path: String, childNodeNameRegex: String) = {
    val childList = listChildren(path)
    val reg = childNodeNameRegex.r
    val dtList = childList.map{
      fs =>
        val name = fs.getPath.getName
        Try {
          val reg(tmpDt) = name
          val dt = if (tmpDt.isEmpty) "0" else tmpDt
          dt.replaceAll("[^\\d]+", "").toLong
        } match {
          case Success(r) => r
          case Failure(e) =>
            e match {
              case _: MatchError => 0L
              case _: Exception => throw e
            }
        }
    }.filter(_ > 0)
    dtList.max
  }

  /**
    * 将一个日期数字转换成指定分隔符的字符串
    * @param dtNum 日期数字，必须是yyyyMMdd形式
    * @param separator 分隔符，比如:"-", "/"
    * @return 返回格式化好的日期字符串
    */
  def dtNum2Str(dtNum: Long, separator: String) = {
    val year = dtNum / 10000
    val tmpMonth = (dtNum % 10000) / 100
    val month = if(tmpMonth < 10) s"0$tmpMonth" else tmpMonth
    val tmpDay = (dtNum % 10000) % 100
    val day = if(tmpDay < 10) s"0$tmpDay" else tmpDay
    s"$year$separator$month$separator$day"
  }

}

