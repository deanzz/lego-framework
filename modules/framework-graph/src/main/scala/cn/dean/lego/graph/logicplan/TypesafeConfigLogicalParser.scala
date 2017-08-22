package cn.dean.lego.graph.logicplan

import com.typesafe.config.{Config, ConfigFactory}
import cn.dean.lego.common._
import cn.dean.lego.common.config.ConfigLoader
import cn.dean.lego.graph.models.GraphNode

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

/**
  * Created by deanzhang on 2017/8/19.
  */
object TypesafeConfigLogicalParser extends GraphLogicalParser[Config, (Config, Option[Config])] {

  override def parse(conf: Config): Seq[GraphNode[(Config, Option[Config])]] = {
    //应用类型，system, application or module
    val confType = conf.getString("type")

    val indexMap = {
      val allMap = mutable.Map.empty[String, (Config, Option[Config])]
      confType match {
        case TYPE_SYSTEM =>
          val sysMap = getFromUpper(conf)
          allMap ++= sysMap
          sysMap.foreach {
            case (idx, (c, _)) =>
              val dir = c.getString("dir")
              val confPath = s"$dir/conf/application.conf"
              val newConf = ConfigLoader.load(confPath, None)
              val appMap = getFromUpper(newConf, Some(idx))
              allMap ++= appMap
              appMap.foreach {
                case (idx1, (c1, _)) =>
                  val dir = c1.getString("dir")
                  val confPath = s"$dir/conf/application.conf"
                  val newConf = ConfigLoader.load(confPath, None)
                  val modMap = getFromModule(newConf, Some(idx1))
                  allMap ++= modMap
              }
          }
        case TYPE_APPLICATION =>
          val appMap = getFromUpper(conf)
          allMap ++= appMap
          appMap.foreach {
            case (idx1, (c1, _)) =>
              val dir = c1.getString("dir")
              val confPath = s"$dir/conf/application.conf"
              val newConf = ConfigLoader.load(confPath, None)
              val modMap = getFromModule(newConf, Some(idx1))
              allMap ++= modMap
          }
        case TYPE_MODULE =>
          val modMap = getFromModule(conf)
          allMap ++= modMap
      }
      allMap
    }

    val nodes = ListBuffer.empty[GraphNode[(Config, Option[Config])]]

    val rootSeq = indexMap.filter(_._1.count(_ == '.') == 0).toSeq.sortBy(_._1.toInt)

    rootSeq.map {
      case (rootIdx, rootConf) =>
        val root = GraphNode(rootIdx, rootConf, ListBuffer.empty[GraphNode[(Config, Option[Config])]], ListBuffer.empty[GraphNode[(Config, Option[Config])]])
        nodes += root
        val childrenMap = indexMap.filter(_._1.startsWith(rootIdx + '.'))
        generateGraphNodes(childrenMap, root, nodes)
        root
    }

    nodes.sortWith(indexOrder)
  }

  private def getFromUpper(conf: Config, parentIdx: Option[String] = None): mutable.Map[String, (Config, Option[Config])] = {
    val seq: Seq[Config] = conf.getConfigList("parts").asScala.filter(c => c.getBoolean("enable"))
    val indexSeq = seq.map {
      c =>
        val index = s"${parentIdx.map(i => i + '.').getOrElse("")}${c.getString("index")}"
        (index, (c, None))
    }
    mutable.Map(indexSeq: _*)
  }

  def getFromModule(conf: Config, parentIdx: Option[String] = None): mutable.Map[String, (Config, Option[Config])] = {
    //获取所有assembly的jar包配置
    val assemblies = conf.getConfigList("assemblies").asScala.filter(c => c.getBoolean("enable"))
    //获取所有assembly的参数配置
    val params = conf.getConfigList("parameters").asScala
    val indexSeq = assemblies.map {
      c =>
        val index = s"${parentIdx.map(i => i + '.').getOrElse("")}${c.getString("index")}"
        val param = params.filter(p => p.getString("name") == c.getString("name")).head
        (index, (c, Some(param)))
    }
    mutable.Map(indexSeq: _*)
  }

  private def isRootLevelNode[T](node: GraphNode[T]) = {
    node.index.count(_ == '.') == 0
  }

  private def generateGraphNodes[T](childrenMap: mutable.Map[String, (Config, Option[Config])], parentNode: GraphNode[(Config, Option[Config])], nodes: ListBuffer[GraphNode[(Config, Option[Config])]]): Unit = {
    //if it is root level nodes
    if(isRootLevelNode(parentNode)){
      val haveReadRoots = nodes.filter(n => isRootLevelNode(n) && n.index != parentNode.index).map(_.index.toInt)
      if(haveReadRoots.nonEmpty){
        val haveReadMaxIdx = haveReadRoots.max
        if(nodes.count(_.index.startsWith(haveReadMaxIdx.toString)) == 1){
          val lastNode = nodes.filter(_.index == haveReadMaxIdx.toString).head
          lastNode.outputs += parentNode
          parentNode.inputs += lastNode
        } else {
          val inputs = nodes.filter(n => n.outputs.isEmpty && n.index.count(_ == '.') > 0 && n.index.startsWith(s"$haveReadMaxIdx."))
          inputs.foreach(n => n.outputs += parentNode)
          parentNode.inputs ++= inputs
        }
      }
    }

    if (childrenMap.nonEmpty) {
      generateHelper(childrenMap, parentNode, nodes)
    }

    def generateHelper(childrenMap: mutable.Map[String, (Config, Option[Config])],
                       parentNode: GraphNode[(Config, Option[Config])],
                       nodes: ListBuffer[GraphNode[(Config, Option[Config])]]): Unit = {
      if (childrenMap.nonEmpty) {
        val childrenNodes = childrenMap.filter {
          case ((key, _)) =>
            val keyArr = key.split('.')
            keyArr.slice(0, keyArr.length - 1).mkString(".") == parentNode.index
        }

        childrenNodes.foreach {
          case (idx, c) =>
            val newNode = GraphNode(idx, c, ListBuffer.empty[GraphNode[(Config, Option[Config])]], ListBuffer.empty[GraphNode[(Config, Option[Config])]])
            parentNode.outputs += newNode
            newNode.inputs += parentNode
            nodes += newNode
            childrenMap -= idx
            generateHelper(childrenMap, newNode, nodes)
        }
      }
    }
  }

  private def arrayOrder(arr1: Array[Int], arr2: Array[Int]): Boolean = {
    //if (arr1.length != arr2.length) throw new UnsupportedOperationException("Arrays with diff length Not supported")
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

  private def indexOrder[T](node1: GraphNode[T], node2: GraphNode[T]): Boolean = {
    val arr1 = node1.index.split('.').map(_.toInt)
    val arr2 = node2.index.split('.').map(_.toInt)
    arrayOrder(arr1, arr2)
  }

  def main(args: Array[String]): Unit = {
    val seq1 = Seq(Array(2), Array(1), Array(4), Array(3), Array(12), Array(7))
    val seq2 = Seq(Array(1, 2), Array(1, 5), Array(1, 4), Array(1, 3), Array(1, 6), Array(1, 1))

    println("seq1")
    seq1.sortWith(arrayOrder).foreach(arr => println(arr.mkString(".")))

    println("seq2")
    seq2.sortWith(arrayOrder).foreach(arr => println(arr.mkString(".")))

    val testConf = ConfigFactory.parseString("{}")

    /*val indexSeq: Seq[(String, Int, Config)] = Seq(
      ("2", 1, testConf),
      ("1", 1, testConf),
      ("2.1", 2, testConf),
      ("1.2", 2, testConf),
      ("1.1", 2, testConf),
      ("2.2", 2, testConf),
      ("2.1.1", 3, testConf),
      ("1.1.1", 3, testConf),
      ("3", 1, testConf)
    ).map(t => (t._1.split(".").map(_.toInt), t._1, t._2, t._3)).
      sortWith((o1, o2) => sameLenArrayOrder(o1._1, o2._1)).
      map(t => (t._2, t._3, t._4))*/

    val map: mutable.Map[String, Config] = mutable.Map(
      ("1.2" -> testConf),
      ("1.1" -> testConf),
      ("1.1.21" -> testConf),
      ("1.22.1" -> testConf),
      ("1.2.12" -> testConf)
    )

    /*val root = GraphNode("1", testConf, Some(ListBuffer.empty[GraphNode[Config]]))
    generateGraphNodes(map, root)

    println(root)*/
  }
}
