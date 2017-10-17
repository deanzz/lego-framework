package cn.dean.lego.common.models

/**
  * Created by deanzhang on 2017/8/24.
  */
object NodeType extends Enumeration{
  type NodeType = Value

  val system, application, module, assembly = Value
}
