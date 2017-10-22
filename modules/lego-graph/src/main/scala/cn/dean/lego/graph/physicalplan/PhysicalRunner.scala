package cn.dean.lego.graph.physicalplan

import akka.actor.Actor
import akka.stream.javadsl.RunnableGraph
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import cn.dean.lego.common.log.Logger
import cn.dean.lego.common.rules.ComponentResult
import cn.dean.lego.graph.physicalplan.NotifyActor.PlanStart
import cn.dean.lego.graph.physicalplan.PhysicalRunner.Run
import org.joda.time.DateTime
import scaldi.{Injectable, Injector}

import scala.concurrent.Future
import scala.util.{Failure, Success}

//todo 下一步Runner会继承一个接口，提高可扩展性
class PhysicalRunner(implicit injector: Injector) extends Actor with Injectable {

  private val logger = inject[Logger]
  private val notifyActor = context.system.actorSelection("akka://lego-framework/user/notification")

  // implicit actor materializer
  private implicit val materializer = ActorMaterializer(ActorMaterializerSettings(context.system).withSupervisionStrategy(decider))

  private val decider: Supervision.Decider = {
    case _: IllegalArgumentException =>
      Supervision.Restart
    case e =>
      val lstTrace = e.getStackTrace.map(_.toString).mkString("\n")
      val err = s"Supervision.Decider: ${e.toString}\n$lstTrace"
      logger.error(err)
      //todo supervision获取的异常和下面future获取的异常是一样的吗？？
      //val result = Seq(ComponentResult("Supervision exception", succeed = false, s"Got exception: $err", None))
      //notifyActor ! result
      Supervision.Stop
  }

  override def receive: Receive = {
    case Run(graph) =>
      val startedAt = DateTime.now
      logger.info(s"start running physicalPlan at ${startedAt.toString("yyyy-MM-dd HH:mm:ss")}, currentTimeMillis = ${startedAt.getMillis}")
      notifyActor ! PlanStart(startedAt)
      import scala.concurrent.ExecutionContext.Implicits.global
      val future = graph.run(materializer)
      future.onComplete{
        case Success(result) =>
          val now = DateTime.now
          val msg = s"finished physicalPlan at ${now.toString("yyyy-MM-dd HH:mm:ss")}, currentTimeMillis = ${now.getMillis}, elapsed time = ${now.getMillis - startedAt.getMillis}ms"
          logger.info(msg)
          notifyActor ! result
        case Failure(e) =>
          val lstTrace = e.getStackTrace.map(_.toString).mkString("\n")
          val err = s"${e.toString}\n$lstTrace"
          logger.error(err)
          val result = Seq(ComponentResult("future exception", succeed = false, s"Got exception: $err", None))
          notifyActor ! result
      }

    case unknown => logger.error(s"PhysicalRunner got unknown message [$unknown]")
  }
}

object PhysicalRunner{
  case class Run(graph: RunnableGraph[Future[Seq[ComponentResult]]])
}
