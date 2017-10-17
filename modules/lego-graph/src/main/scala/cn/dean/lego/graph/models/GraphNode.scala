package cn.dean.lego.graph.models

import cn.dean.lego.common.models.NodeType
import cn.dean.lego.common.rules.Component

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by deanzhang on 2017/8/19.
  */

case class GraphNode[T](id: Long, info: T, inputs: ListBuffer[GraphNode[T]], outputs: ListBuffer[GraphNode[T]]) {
  @Override
  override def toString: String = {
    info match {
      case v: NodeProp =>
        s"$id#${v.name}#${v.nodeType}#in:[${inputs.map(_.info.asInstanceOf[NodeProp].name).mkString(";")}]#out:[${outputs.map(_.info.asInstanceOf[NodeProp].name).mkString(";")}]"//#in2:[${inputs.map(_.id).mkString(";")}]#out2:[${outputs.map(_.id).mkString(";")}]
      case _ => s"id: $id, inputs: [${inputs.map(_.id).mkString("#")}], outputs: [${outputs.map(_.id).mkString("#")}]"
    }

  }
}

