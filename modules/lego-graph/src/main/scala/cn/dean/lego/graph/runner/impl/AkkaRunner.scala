package cn.dean.lego.graph.runner.impl

import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import akka.stream.scaladsl.RunnableGraph
import cn.dean.lego.common.rules.ComponentResult
import cn.dean.lego.graph.physicalplan.NotifyActor.PlanStart
import cn.dean.lego.graph.runner.Runner
import org.joda.time.DateTime
import scaldi.Injector

import scala.concurrent.Future
import scala.util.{Failure, Success}

class AkkaRunner(implicit injector: Injector) extends Runner[RunnableGraph[Future[Seq[ComponentResult]]], Unit]{

  private val notifyActor = context.system.actorSelection("akka://lego-framework/user/notification")

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

  // implicit actor materializer
  private implicit val materializer = ActorMaterializer(ActorMaterializerSettings(context.system).withSupervisionStrategy(decider))

  override def run(graph: RunnableGraph[Future[Seq[ComponentResult]]]): Unit = {
    val startedAt = DateTime.now
    logger.info(s"start running physicalPlan at ${startedAt.toString("yyyy-MM-dd HH:mm:ss")}, currentTimeMillis = ${startedAt.getMillis}")
    notifyActor ! PlanStart(startedAt)
    import scala.concurrent.ExecutionContext.Implicits.global
    val future = graph.run
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
  }
}
