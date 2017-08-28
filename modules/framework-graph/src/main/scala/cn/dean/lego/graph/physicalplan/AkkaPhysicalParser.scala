package cn.dean.lego.graph.physicalplan

import akka.NotUsed
import akka.actor.{Actor, ActorSystem, Props}
import akka.stream._
import akka.stream.javadsl.RunnableGraph
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Merge, Sink, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import cn.dean.lego.common.exceptions.FlowRunFailedException
import cn.dean.lego.common.loader.ComponentLoader
import cn.dean.lego.common.models.NodeType
import cn.dean.lego.common.rules.ComponentResult
import cn.dean.lego.graph.models.GraphNode
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import scaldi.Injectable.inject
import scaldi.Injector

/**
  * Created by deanzhang on 2017/8/22.
  */

class AkkaPhysicalParser(implicit injector: Injector) extends GraphPhysicalParser[(Config, Option[Config]), RunnableGraph[(NotUsed, NotUsed)]] {

  implicit val actorSystem: ActorSystem = inject[ActorSystem]

  val decider: Supervision.Decider = {
    case _: IllegalArgumentException => Supervision.Restart
    case e =>
      val result = ComponentResult(succeed = false, s"Got exception: $e", None)
      // actor ! result
      Supervision.Stop
    /*val stop = Supervision.Stop
    system.terminate()
    stop*/
  }

  // implicit actor materializer
  implicit val materializer = ActorMaterializer(ActorMaterializerSettings(actorSystem).withSupervisionStrategy(decider))

  override def parse(sc: SparkContext, nodes: Seq[GraphNode[(Config, Option[Config])]]): RunnableGraph[(NotUsed, NotUsed)] = {

    val source = Source.single(Seq(ComponentResult(succeed = true, "start", None)))
    //todo 需要创建sinkActor
    val sink = Sink.actorRef(null, onCompleteMessage = "finished")
    RunnableGraph.fromGraph(GraphDSL.create(source, sink)((_, _)) {
      implicit builder =>
        (src, snk) =>
          import GraphDSL.Implicits._
          val flowMap = nodes.tail.map {
            n =>
              val flow = generateFlow(sc, n.nodeType, n.info._1, n.info._2)
              (n.index, flow)
          }.toMap

          nodes.foreach {
            n =>
              val currFlow = builder.add(flowMap(n.index))

              val inputs = n.inputs.map(i => flowMap(i.index))
              if (inputs.isEmpty) {
                src ~> currFlow
              } else {
                if (inputs.length > 1) {
                  val merge = builder.add(Merge[Seq[ComponentResult]](inputs.length))
                  inputs.indices.foreach {
                    idx =>
                      builder.add(inputs(idx)) ~> merge.in(idx)
                  }
                  merge.out ~> currFlow
                } else {
                  builder.add(inputs.head) ~> currFlow
                }
              }

              val outputs = n.outputs.map(o => flowMap(o.index))
              if (outputs.isEmpty) {
                currFlow ~> snk
              } else {
                if (outputs.length > 1) {
                  val broadcast = builder.add(Broadcast[Seq[ComponentResult]](outputs.length))
                  currFlow ~> broadcast
                  outputs.indices.foreach {
                    idx =>
                      broadcast.out(idx) ~> builder.add(outputs(idx).async)
                  }
                } else {
                  currFlow ~> builder.add(outputs.head)
                }
              }
          }
          ClosedShape
    })

    //g.run(materializer)
  }

  /*
       val f1 = Flow[Seq[ComponentResult]].map{
          lastResult =>
            val name = "name"
            val index = "index"
            val nodeType = NodeType.assembly
            nodeType match {
              case NodeType.assembly =>
                val jarName = "jar-name"
                val className = "class-name"
                val assembly = ComponentLoader.load(jarName, className)
                val lastRdd = lastResult.head.result
                //执行assembly，获得结果
                val Some(currResult) = assembly.run(sc, null, lastRdd)
                if(!currResult.succeed){
                  throw FlowRunFailedException(s"$nodeType [$index-$name] run failed, message: \n${currResult.message}")
                } else Seq(currResult)
              case _ =>
                Seq(ComponentResult(succeed = true, s"$nodeType [$index-$name]", None))
            }
        }

        val f2 = Flow[Seq[ComponentResult]].map{
          lastResult =>
            val name = "name"
            val index = "index"
            val nodeType = NodeType.assembly
            nodeType match {
              case NodeType.assembly =>
                val jarName = "jar-name"
                val className = "class-name"
                val assembly = ComponentLoader.load(jarName, className)
                val lastRdd = lastResult.head.result
                //执行assembly，获得结果
                val Some(currResult) = assembly.run(sc, null, lastRdd)
                if(!currResult.succeed){
                  throw FlowRunFailedException(s"$nodeType [$index-$name] run failed, message: \n${currResult.message}")
                } else Seq(currResult)
              case _ =>
                Seq(ComponentResult(succeed = true, s"$nodeType [$index-$name]", None))
            }
        }

        val source = builder.add(Source.single(Seq(ComponentResult(true, "", None))))

        val flow1 = builder.add(f1)
        val flow2 = builder.add(f2)

        val broadcast = builder.add(Broadcast[Seq[ComponentResult]](2))

        val merge = builder.add(Merge[Seq[ComponentResult]](2))

        flow1 ~> broadcast ~> flow1 ~> merge ~> flow2
                           broadcast ~> flow2 ~> merge*/

  /*private def generateShape(structConf: Config, paramConf: Option[Config]): GraphStage[FlowShape[ComponentResult, ComponentResult]] = {
    new GraphStage[FlowShape[ComponentResult, ComponentResult]] {
      val in = Inlet[ComponentResult]("flowStage.in")
      val out = Outlet[ComponentResult]("flowStage.out")

      override val shape = FlowShape(in, out)

      override def createLogic(inheritedAttributes: Attributes) = new GraphStageLogic(shape) {
        setHandler(in, new InHandler {
          override def onPush() = grab(in) match {
            case lastResult ⇒
              val isAssembly = !structConf.getConfig("jar-name").isEmpty
              val newResult = if (isAssembly) {

              } else {

              }
              push(out, newResult)

          }
        })
        setHandler(out, new OutHandler {
          override def onPull() = pull(in)
        })
      }
    }
  }*/

  private def generateFlow(sc: SparkContext, nodeType: NodeType.Value, structConf: Config, paramConf: Option[Config]): Flow[Seq[ComponentResult], Seq[ComponentResult], NotUsed] /*: FlowShape[Seq[ComponentResult], Seq[ComponentResult]]*/ = {
    Flow[Seq[ComponentResult]].map {
      lastResult =>
        val name = structConf.getString("name")
        val index = structConf.getString("index")
        nodeType match {
          case NodeType.assembly =>
            val jarName = structConf.getString("jar-name")
            val className = structConf.getString("class-name")
            val assembly = ComponentLoader.load(jarName, className)
            val lastRdd = lastResult.head.result
            //执行assembly，获得结果
            // todo 这里的lastRdd可以是个Map结构，适应merge的情况
            val Some(currResult) = assembly.run(sc, paramConf, lastRdd)
            if (!currResult.succeed) {
              throw FlowRunFailedException(s"$nodeType [$index-$name] run failed, message: \n${currResult.message}")
            } else Seq(currResult)
          case _ =>
            Seq(ComponentResult(succeed = true, s"$nodeType [$index-$name]", None))
        }
    }
  }
}
