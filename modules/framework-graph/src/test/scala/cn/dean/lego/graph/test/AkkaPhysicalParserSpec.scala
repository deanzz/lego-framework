
package cn.dean.lego.graph.test

import cn.dean.lego.graph.logicplan.TypesafeConfigLogicalParser
import cn.dean.lego.graph.module.GraphModule
import cn.dean.lego.graph.physicalplan.AkkaPhysicalParser
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import scaldi.Injectable.inject
import org.scalatest.FlatSpec


class AkkaPhysicalParserSpec extends FlatSpec{

  private val sysConf = "/Users/deanzhang/work/code/github/lego-framework/sample/s1/conf/application.conf"
  implicit val injector = new GraphModule(sysConf)

  private val physicalParser = inject[AkkaPhysicalParser]

  private def genGraphNodes = {
    val logicalParser = inject[TypesafeConfigLogicalParser]
    logicalParser.parse(inject[Config])
  }

  it should "run system succeed" in {
    val nodes = genGraphNodes
    nodes.foreach(println)
    physicalParser.run(inject[SparkContext], nodes)

    assert(nodes.length === 32)
  }

  /*it should "test succeed" in {
    physicalParser.test()
  }*/

}

