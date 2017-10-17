package cn.dean.lego.core

import cn.dean.lego.common.log.Logger
import org.apache.spark.SparkContext
import cn.dean.lego.graph.logicplan.TypesafeConfigLogicalParser
import cn.dean.lego.graph.module.GraphModule
import cn.dean.lego.graph.physicalplan.AkkaPhysicalParser
import com.typesafe.config.Config
import scaldi.Injectable.inject

import scala.util.{Failure, Success, Try}

/**
  * Created by deanzhang on 17/10/2.
  */
object Launcher {

  /**
    * 入口函数
    * @param args 入口函数参数，目前有一个参数，配置文件绝对路径（local/hdfs）
    */
  def main(args: Array[String]): Unit = {
    //获取配置文件路径
    val configPath =
      if (args.length == 1)
        args(0)
      else {
        "conf/application.conf"
      }
    implicit val injector = new GraphModule(configPath)
    val logicalParser = inject[TypesafeConfigLogicalParser]
    val logicalNodes = logicalParser.parse(inject[Config])
    val physicalParser = inject[AkkaPhysicalParser]
    val logger = inject[Logger]
    val sc = inject[SparkContext]
    Try {
      physicalParser.run(sc, logicalNodes)
    } match {
      case Success(res) => res
      case Failure(e) =>
        val lstTrace = e.getStackTrace.map(_.toString).mkString("\n")
        val err = s"${e.toString}\n$lstTrace"
        logger.error(err)
    }

  }
}
