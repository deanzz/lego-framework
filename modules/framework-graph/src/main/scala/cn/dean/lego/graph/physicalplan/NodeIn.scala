package cn.dean.lego.graph.physicalplan

import akka.stream.Inlet

class NodeIn[T](in: Inlet[T]) {

  val inlets: Inlet[T] = in

  private var connected: Boolean = false

  def connect(): Unit = connected = true

  def isConnected: Boolean = connected

}
