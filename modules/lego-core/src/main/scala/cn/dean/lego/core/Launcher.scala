package cn.dean.lego.core

import akka.actor.ActorSystem
import cn.dean.lego.common.log.Logger
import cn.dean.lego.graph.logicplan.TypesafeConfigLogicalParser
import cn.dean.lego.graph.module.GraphModule
import cn.dean.lego.graph.physicalplan.{AkkaPhysicalParser, PhysicalRunner}
import com.typesafe.config.Config
import scaldi.Injectable.inject
import scaldi.akka.AkkaInjectable._
import akka.pattern.ask
import akka.util.Timeout
import cn.dean.lego.graph.physicalplan.PhysicalRunner.Run

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

/**
  * Created by deanzhang on 17/10/2.
  */
object Launcher {

  /**
    * 入口函数
    *
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
    val logger = inject[Logger]
    implicit val actorSystem: ActorSystem = inject[ActorSystem]
    try {
      val logicalParser = inject[TypesafeConfigLogicalParser]
      val logicalNodes = logicalParser.parse(inject[Config])
      val physicalParser = inject[AkkaPhysicalParser]
      val physicalGraph = physicalParser.parse(logicalNodes)
      val physicalRunner = injectActorRef[PhysicalRunner]("physical-runner")
      logger.info(s"Start up physicalRunnerActor [${physicalRunner.path}]")
      implicit val timeout: Timeout = Timeout(10.day)
      val future = physicalRunner ? Run(physicalGraph)
      //Await.result(future, 5.day)
      Await.ready(future, 10.day)
    } catch {
      case e: Exception =>
        val lstTrace = e.getStackTrace.map(_.toString).mkString("\n")
        val err = s"${e.toString}\n$lstTrace"
        logger.error(err)
    }

  }
}
