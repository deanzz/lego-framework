package cn.dean.lego.graph.logicplan

import com.typesafe.config.{Config, ConfigValueFactory}
import cn.dean.lego.common.config.{ConfigLoader, KeyValue}
import cn.dean.lego.common.log.Logger
import cn.dean.lego.common.models.NodeType
import cn.dean.lego.graph.models.{GraphNode, NodeProp}
import scaldi.Injectable.inject
import scaldi.Injector

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

/**
  * Created by deanzhang on 2017/8/19.
  */
class TypesafeConfigLogicalParser(implicit injector: Injector) extends GraphLogicalParser[Config, NodeProp] {

  private val logger = inject[Logger]

  override def parse(conf: Config): Seq[GraphNode[NodeProp]] = {
    val nodes = extractNodes(conf)
    logger.info("logicalPlan:")
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

  private def extractNodes(config: Config): Seq[GraphNode[NodeProp]] = {
    val confType = config.getString("type")
    //获取全局上下文参数
    val contextParameters: Seq[KeyValue] = config.getConfigList("context.parameters").asScala.map(KeyValue.apply)
    val nodeMap = mutable.Map.empty[Long, Seq[GraphNode[NodeProp]]]
    val rootNodes = confType match {
      case "system" =>
        extractSystem(config, nodeMap, contextParameters)
      case "application" =>
        extractApplication(config, nodeMap, contextParameters)
      case "module" =>
        extractModule(config, contextParameters)
    }

    linkSubcomponents(rootNodes, nodeMap)
  }


  private def extractModule(conf: Config, contextParameters: Seq[KeyValue]) = {
    val assemblySeq = getNodeProps(conf)
    val nodes = ListBuffer.empty[GraphNode[NodeProp]]
    val baseId = System.nanoTime()
    var idx = 0
    assemblySeq.map {
      asmblyProp =>
        val id = baseId + idx
        var param = asmblyProp.paramConf.get
        contextParameters.foreach{
          kv =>
            param = param.withValue(kv.key, ConfigValueFactory.fromAnyRef(kv.value))
        }
        val n = newNode(asmblyProp.copy(paramConf = Option(param)), id, idx, nodes)
        nodes += n
        idx += 1
        n
    }
  }

  private def extractApplication(conf: Config, nodeMap: mutable.Map[Long, Seq[GraphNode[NodeProp]]], contextParameters: Seq[KeyValue]) = {
    val moduleSeq = getNodeProps(conf)
    var idx = 0
    val baseId = System.nanoTime()
    val nodes = ListBuffer.empty[GraphNode[NodeProp]]
    moduleSeq.map {
      moduleProp =>
        val moduleId = baseId + idx
        val n = newNode(moduleProp, moduleId, idx, nodes)
        nodes += n
        idx += 1
        val moduleSeq = extractModule(moduleProp.structConf, contextParameters)
        nodeMap += (moduleId -> moduleSeq)
        n
    }
  }

  private def extractSystem(conf: Config, nodeMap: mutable.Map[Long, Seq[GraphNode[NodeProp]]], contextParameters: Seq[KeyValue]) = {
    val appSeq = getNodeProps(conf)
    var idx = 0
    val baseId = System.nanoTime()
    val nodes = ListBuffer.empty[GraphNode[NodeProp]]
    appSeq.map {
      appProp =>
        val appId = baseId + idx
        val n = newNode(appProp, appId, idx, nodes)
        nodes += n
        idx += 1
        val appSeq = extractApplication(appProp.structConf, nodeMap, contextParameters)
        nodeMap += (appId -> appSeq)
        n
    }
  }

  private def linkSubcomponents(rootNodeSeq: Seq[GraphNode[NodeProp]], nodeMap: mutable.Map[Long, Seq[GraphNode[NodeProp]]]) = {

    def link(nodeSeq: Seq[GraphNode[NodeProp]]) = {
      val subNodesInOut = nodeSeq.map{
        n =>
          val subNodes = nodeMap(n.id)
          val outputs = n.outputs.flatMap(o => nodeMap(o.id))
          val inputs = n.inputs.flatMap(o => nodeMap(o.id))
          val subNodesIn = subNodes.filter(_.inputs.isEmpty)
          val subNodesOut = subNodes.filter(_.outputs.isEmpty)
          val inlets = inputs.filter(_.outputs.isEmpty)
          val outlets = outputs.filter(_.inputs.isEmpty)
          (subNodes, subNodesIn, subNodesOut, inlets, outlets)
      }

      subNodesInOut.flatMap{
        case (subNodes, subNodesIn, subNodesOut, inlets, outlets) =>
          subNodesIn.foreach{
            o =>
              inlets.foreach{
                i =>
                  if(!o.inputs.contains(i))
                    o.inputs += i
              }
          }
          subNodesOut.foreach{
            o =>
              outlets.foreach{
              i =>
                if(!o.outputs.contains(i))
                  o.outputs += i
            }
          }
          inlets.foreach{
            o =>
              subNodesIn.foreach{
                i =>
                  if(!o.outputs.contains(i))
                    o.outputs += i
              }
          }
          outlets.foreach{
            o =>
              subNodesOut.foreach{
                i =>
                  if(!o.inputs.contains(i))
                    o.inputs += i
              }
          }
          subNodes
      }
    }

    val rootType = rootNodeSeq.head.info.nodeType
    rootType match {
      case NodeType.application =>
        val moduleSeq = link(rootNodeSeq)
        /*println("moduleSeq")
        moduleSeq.foreach(println)*/
        val asmblySeq = link(moduleSeq)
        /*println()
        println("asmblySeq")
        asmblySeq.foreach(println)*/
        asmblySeq
      case NodeType.module =>
        link(rootNodeSeq)
      case _ => rootNodeSeq
    }
  }

  private def getPrevNodes(nodes: ListBuffer[GraphNode[NodeProp]], currIdx: Int, currNodeProp: NodeProp) = {
    if (nodes.isEmpty || currIdx <= 0) Seq.empty[GraphNode[NodeProp]] else {
      val prevIdx = currIdx - 1
      val prevNode = nodes(prevIdx)
      if (currNodeProp.originIndex.contains('.')/* && prevNode.info.nodeType == currNodeProp.nodeType*/) {
        Seq(nodes.filter(_.info.originIndex == currNodeProp.originIndex.substring(0, currNodeProp.originIndex.lastIndexOf('.'))).maxBy(_.id))
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

  private def newNode(nodeProp: NodeProp, id: Long, index: Int, nodes: ListBuffer[GraphNode[NodeProp]]) = {
    val prevNodes = getPrevNodes(nodes, index, nodeProp)
    val inputs = if (prevNodes.nonEmpty) ListBuffer(prevNodes: _*) else ListBuffer.empty[GraphNode[NodeProp]]
    val n = GraphNode(id, nodeProp, inputs, ListBuffer.empty[GraphNode[NodeProp]])
    if (prevNodes.nonEmpty) prevNodes.foreach(p => p.outputs += n)
    n
  }

  private def getNodeProps(conf: Config): Seq[NodeProp] = {
    val isModule = Try(conf.getConfigList("assemblies") != null).getOrElse(false)
    if (isModule) {
      val assemblyDir = conf.getString("assemblies-dir")
      val assemblies = conf.getConfigList("assemblies").asScala.filter(c => c.getBoolean("enable")).map(_.withValue("assemblies-dir", ConfigValueFactory.fromAnyRef(assemblyDir))).sortWith {
        case (c1, c2) =>
          val a1 = c1.getString("index").split(".").map(_.toInt)
          val a2 = c2.getString("index").split(".").map(_.toInt)
          arrayOrder(a1, a2)
      }
      val params = conf.getConfigList("parameters").asScala
      assemblies.map {
        c =>
          val idx = c.getString("index")
          val name = c.getString("name")
          val param = Option(params.filter(p => p.getString("name") == c.getString("name")).head)
          NodeProp(idx, name, NodeType.assembly, c, param)
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
          val name = c.getString("name")
          NodeProp(idx, name, nodeType, newConf)
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


