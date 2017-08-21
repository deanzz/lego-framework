package cn.dean.lego.graph.models

import cn.dean.lego.common.rules.Component

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by deanzhang on 2017/8/19.
  */
//case class GraphNode[T](index: String, level: Int, info: T, children: Option[mutable.Map[String, GraphNode[T]]] = None)
case class GraphNode[T](index: Index, info: T, children: Option[ListBuffer[GraphNode[T]]] = None){
  @Override
  override def toString: String = {
    val res = mutable.StringBuilder.newBuilder
    res.append(index.num).append("\n")
    def loop(node: GraphNode[T]): Unit = {
      if(node.children.nonEmpty){
        node.children.get.foreach{
          n =>
            res.append(n.index.num).append("\n")
            loop(n)
        }
      }
    }

    loop(this)
    res.toString()
  }
}

case class Index(num: String, level: Int)