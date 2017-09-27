package cn.dean.lego.graph.physicalplan

import akka.stream.Outlet

class NodeOut[T](out: Outlet[T]) {

  val outlets: Outlet[T] = out

  private var connected: Boolean = false

  def connect(): Unit = connected = true

  def isConnected: Boolean = connected

}
