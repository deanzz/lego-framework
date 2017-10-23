package cn.dean.lego.graph.runner

import akka.actor.Actor
import cn.dean.lego.common.log.Logger
import cn.dean.lego.graph.runner.Runner.Run
import scaldi.akka.AkkaInjectable
import scaldi.Injector

abstract class Runner[I, O](implicit injector: Injector) extends Actor with AkkaInjectable {

  protected val logger: Logger = inject[Logger]

  override def receive: Receive = {
    case Run(graph: I) =>
      run(graph)

    case unknown => logger.error(s"Runner got unknown message [$unknown]")
  }

  def run(graph: I): O
}

object Runner{
  case class Run[I](graph: I)
}

