package cn.dean.lego.graph.models

import cn.dean.lego.common.models.NodeType
import com.typesafe.config.Config

case class NodeProp(originIndex: String, name: String, nodeType: NodeType.Value, structConf: Config, paramConf: Option[Config] = None)
