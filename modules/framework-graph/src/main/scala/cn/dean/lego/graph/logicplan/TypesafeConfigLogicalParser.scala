package cn.dean.lego.graph.logicplan

import com.typesafe.config.Config
import cn.dean.lego.common.config.ConfigLoader
import cn.dean.lego.common.log.Logger
import cn.dean.lego.common.models.NodeType
import cn.dean.lego.graph.models.{GraphNode, NodeProp}
import scaldi.Injectable.inject
import scaldi.Injector

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._
import scala.util.Try

/**
  * Created by deanzhang on 2017/8/19.
  */
class TypesafeConfigLogicalParser(implicit injector: Injector) extends GraphLogicalParser[Config, NodeProp] {

  private val logger = inject[Logger]

  override def parse(conf: Config): Seq[GraphNode[NodeProp]] = {
    val nodes = extractNodes(conf)
    nodes.foreach(n => logger.info(n.toString))
    nodes
  }

  private def getNodeType(confType: String) = {
    confType match {
      case "system" => NodeType.application
      case "application" => NodeType.module
      case _ => NodeType.assembly
    }
  }

  private def getPrevNodes(nodes: ListBuffer[GraphNode[NodeProp]], currIdx: Int, currNodeProp: NodeProp) = {
    if (currIdx <= 0) Seq.empty[GraphNode[NodeProp]] else {
      val prevIdx = currIdx - 1
      val prevNode = nodes(prevIdx)
      if (currNodeProp.originIndex.contains('.') && prevNode.info.nodeType == currNodeProp.nodeType) {
        Seq(nodes.filter(_.info.originIndex == currNodeProp.originIndex.substring(0, currNodeProp.originIndex.lastIndexOf('.'))).maxBy(_.index))
      } else {
        if (prevNode.info.originIndex.contains('.')) {
          val num = prevNode.info.originIndex.substring(prevNode.info.originIndex.lastIndexOf('.') + 1, prevNode.info.originIndex.length).toInt
          (0 until num).map {
            i =>
              val idx = prevIdx - i
              nodes(idx)
          }
        } else Seq(prevNode)
      }
    }
  }


  private def extractNodes(config: Config): ListBuffer[GraphNode[NodeProp]] = {
    val confType = config.getString("type")
    val nodes = ListBuffer.empty[GraphNode[NodeProp]]
    var globalIdx = 0
    confType match {
      case "system" =>
        val lastIdx = extractSystem(config, globalIdx, nodes)
        globalIdx += lastIdx
      case "application" =>
        val lastIdx = extractApplication(config, globalIdx, nodes)
        globalIdx += lastIdx
      case "module" =>
        val lastIdx = extractModule(config, globalIdx, nodes)
        globalIdx += lastIdx
    }
    nodes
  }

  private def extractModule(conf: Config, globalIdx: Int, nodes: ListBuffer[GraphNode[NodeProp]]): Int = {
    val assemblySeq = getNodeProps(conf)
    var idx = globalIdx
    assemblySeq.foreach {
      asmblyProp =>
        val n = newNode(asmblyProp, idx, nodes)
        nodes += n
        idx += 1
    }
    idx
  }

  private def extractApplication(conf: Config, globalIdx: Int, nodes: ListBuffer[GraphNode[NodeProp]]) = {
    val moduleSeq = getNodeProps(conf)
    var idx = globalIdx
    moduleSeq.foreach {
      moduleProp =>
        val n = newNode(moduleProp, idx, nodes)
        nodes += n
        idx += 1
        val lastIdx = extractModule(moduleProp.structConf, idx, nodes)
        idx = lastIdx
    }
    idx
  }

  private def extractSystem(conf: Config, globalIdx: Int, nodes: ListBuffer[GraphNode[NodeProp]]) = {
    val appSeq = getNodeProps(conf)
    var idx = globalIdx
    appSeq.foreach {
      appProp =>
        val n = newNode(appProp, idx, nodes)
        nodes += n
        idx += 1
        val lastIdx = extractApplication(appProp.structConf, idx, nodes)
        idx = lastIdx
    }
    idx
  }


  private def newNode(nodeProp: NodeProp, globalIdx: Int, nodes: ListBuffer[GraphNode[NodeProp]]) = {
    val prevNodes = getPrevNodes(nodes, globalIdx, nodeProp)
    val inputs = if (prevNodes.nonEmpty) ListBuffer(prevNodes: _*) else ListBuffer.empty[GraphNode[NodeProp]]
    val n = GraphNode(globalIdx, nodeProp, inputs, ListBuffer.empty[GraphNode[NodeProp]])
    if (prevNodes.nonEmpty) prevNodes.foreach(p => p.outputs += n)
    n
  }

  private def getNodeProps(conf: Config): Seq[NodeProp] = {
    val isModule = Try(conf.getConfigList("assemblies") != null).getOrElse(false)
    if (isModule) {
      val assemblies = conf.getConfigList("assemblies").asScala.filter(c => c.getBoolean("enable")).sortWith {
        case (c1, c2) =>
          val a1 = c1.getString("index").split(".").map(_.toInt)
          val a2 = c2.getString("index").split(".").map(_.toInt)
          arrayOrder(a1, a2)
      }
      val params = conf.getConfigList("parameters").asScala
      assemblies.map {
        c =>
          val idx = c.getString("index")
          val param = Option(params.filter(p => p.getString("name") == c.getString("name")).head)
          NodeProp(idx, NodeType.assembly, c, param)
      }
    } else {
      val confType = conf.getString("type")
      val nodeType = getNodeType(confType)
      val seq: Seq[Config] = conf.getConfigList("parts").asScala.filter(c => c.getBoolean("enable")).sortWith {
        case (c1, c2) =>
          val a1 = c1.getString("index").split(".").map(_.toInt)
          val a2 = c2.getString("index").split(".").map(_.toInt)
          arrayOrder(a1, a2)
      }
      seq.map {
        c =>
          val dir = c.getString("dir")
          val confPath = s"$dir/conf/application.conf"
          val newConf = ConfigLoader.load(confPath, None)
          val idx = c.getString("index")
          NodeProp(idx, nodeType, newConf)
      }
    }
  }

  private def arrayOrder(arr1: Array[Int], arr2: Array[Int]): Boolean = {
    import scala.util.control.Breaks.{break, breakable}
    val len = math.min(arr1.length, arr2.length)
    var result = false
    breakable {
      (0 until len).foreach {
        idx =>
          if (arr1(idx) != arr2(idx)) {
            result = arr1(idx) < arr2(idx)
            break()
          }
      }
    }
    result
  }
}


