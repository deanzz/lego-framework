package cn.dean.lego.graph.physicalplan

import akka.stream.scaladsl.RunnableGraph
import cn.dean.lego.graph.models.GraphNode

/**
  * Created by deanzhang on 2017/8/20.
  */
trait GraphPhysicalParser[I, O] {

  def parse(nodes: Seq[GraphNode[I]]): O

}
