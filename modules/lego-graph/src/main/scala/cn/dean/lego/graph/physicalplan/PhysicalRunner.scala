package cn.dean.lego.graph.physicalplan

import akka.actor.Actor
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import cn.dean.lego.common.log.Logger
import cn.dean.lego.common.rules.ComponentResult
import cn.dean.lego.graph.models.{GraphNode, NodeProp}
import cn.dean.lego.graph.physicalplan.PhysicalRunner.Run
import org.apache.spark.SparkContext
import scaldi.Injectable.inject
import scaldi.Injector
import scaldi.akka.AkkaInjectable

class PhysicalRunner(implicit injector: Injector) extends Actor with AkkaInjectable {

  private val physicalParser = inject[AkkaPhysicalParser]
  private val logger = inject[Logger]
  private val notifyActor = injectActorRef[NotifyActor]
  logger.info(s"Start up notifyActor [${notifyActor.path}]")
  // implicit actor materializer
  private implicit val materializer = ActorMaterializer(ActorMaterializerSettings(context.system).withSupervisionStrategy(decider))

  private var isStopped = false
  private val decider: Supervision.Decider = {
    case _: IllegalArgumentException =>
      Supervision.Restart
    case e =>
      val lstTrace = e.getStackTrace.map(_.toString).mkString("\n")
      val err = s"Supervision.Decider: ${e.toString}\n$lstTrace"
      logger.error(err)
      //val result = Seq(ComponentResult("got~exception", succeed = false, s"Got exception: $err", None))
      //notifyActor ! result
      Supervision.Stop
  }

  override def receive: Receive = {
    case Run(nodes) =>
      val graph = physicalParser.parse(nodes)
      val future = graph.run(materializer)
      import scala.concurrent.ExecutionContext.Implicits.global
      future.onSuccess{
        case result =>
          logger.info("future.onSuccess")
          notifyActor ! result
      }

      future.onFailure{
        case e =>
          val lstTrace = e.getStackTrace.map(_.toString).mkString("\n")
          val err = s"${e.toString}\n$lstTrace"
          logger.error(err)
          val result = Seq(ComponentResult("got~exception", succeed = false, s"Got exception: $err", None))
          notifyActor ! result
      }

    case unknown => logger.error(s"PhysicalRunner got unknown message [$unknown]")
  }
}

object PhysicalRunner{
  case class Run(nodes: Seq[GraphNode[NodeProp]])
}
