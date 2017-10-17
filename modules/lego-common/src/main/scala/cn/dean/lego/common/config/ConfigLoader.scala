package cn.dean.lego.common.config

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkContext
import org.slf4j.{Logger, LoggerFactory}

import scala.io.Source

/**
  * Created by deanzhang on 15/11/29.
  */

/**
  * 配置加载器
  */
object ConfigLoader {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def load(sc: SparkContext, path: String): Config = {

    val content = if(path.trim.toLowerCase.startsWith("hdfs://")){
      sc.textFile(path).collect().mkString("\n")
    } else {
      Source.fromFile(path).getLines().mkString("\n")
    }

    logger.info("config file path = " + path)
    load(content)
  }

  def load(path: String, sc: Option[SparkContext] = None): Config = {

    val content = if(path.trim.toLowerCase.startsWith("hdfs://")){
      sc.get.textFile(path).collect().mkString("\n")
    } else {
      Source.fromFile(path).getLines().mkString("\n")
    }

    logger.info("config file path = " + path)
    load(content)
  }

  def load(content: String): Config = {
    ConfigFactory.parseString(content)
  }

/*  def load(file: File): Config = {
    logger.info("config file path = " + file.getAbsolutePath)
    ConfigFactory.parseFile(file)
  }*/

}
