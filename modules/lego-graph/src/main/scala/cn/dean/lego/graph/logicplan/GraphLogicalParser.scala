package cn.dean.lego.graph.logicplan

import cn.dean.lego.graph.models.GraphNode

/**
  * Created by deanzhang on 2017/8/19.
  */
trait GraphLogicalParser[I, O] {

  def parse(conf: I): Seq[GraphNode[O]]

}
