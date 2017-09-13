package cn.dean.lego.graph.physicalplan

import akka.NotUsed
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.stream._
import akka.stream.javadsl.RunnableGraph
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Merge, Sink, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import cn.dean.lego.common.exceptions.FlowRunFailedException
import cn.dean.lego.common.loader.ComponentLoader
import cn.dean.lego.common.log.Logger
import cn.dean.lego.common.models.NodeType
import cn.dean.lego.common.rules.ComponentResult
import cn.dean.lego.graph.models.{GraphNode, NodeProp}
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import scaldi.Injectable.inject
import scaldi.Injector
import scaldi.akka.AkkaInjectable

/**
  * Created by deanzhang on 2017/8/22.
  */

class AkkaPhysicalParser(implicit injector: Injector) extends GraphPhysicalParser[NodeProp, RunnableGraph[NotUsed]] {

  private implicit val actorSystem: ActorSystem = inject[ActorSystem]
  private val logger = inject[Logger]

  private val notifyActor = AkkaInjectable.injectActorRef[NotifyActor]("notifyActor")
  logger.info("Start up notifyActor")

  private val decider: Supervision.Decider = {
    case _: IllegalArgumentException =>
      Supervision.Restart
    case e =>
      val lstTrace = e.getStackTrace.map(_.toString).mkString("\n")
      val err = s"${e.toString}\n$lstTrace"
      logger.error(err)
      val result = ComponentResult(succeed = false, s"Got exception: $err", None)
      val stop = Supervision.Stop
      notifyActor ! result
      stop
  }

  // implicit actor materializer
  private implicit val materializer = ActorMaterializer(ActorMaterializerSettings(actorSystem).withSupervisionStrategy(decider))

  override def parse(sc: SparkContext, nodes: Seq[GraphNode[NodeProp]]): RunnableGraph[NotUsed] = {

    val source = Source.single(Seq(ComponentResult(succeed = true, "start", None)))
    val sink = Sink.actorRef(notifyActor, onCompleteMessage = "notify finished")
    RunnableGraph.fromGraph(GraphDSL.create() {
      implicit builder =>
        //(src, snk) =>
        import GraphDSL.Implicits._
        val flowMap = nodes.map {
          n =>
            val flow = builder.add(generateFlow(sc, n.info.nodeType, n.info.structConf, n.info.paramConf))
            (n.index, flow)
        }.toMap

        /*nodes.foreach{
          n =>
            val flow = flowMap(n.index)
            val input = n.inputs.map(i => flowMap(i.index))
            if(input.isEmpty)
              builder.add(source) ~> flow
           /* else
              input.head ~> flow*/

            val output = n.outputs.map(o => flowMap(o.index))

            if(output.isEmpty)
              flow ~> builder.add(sink)
            else
              flow ~> output.head

        }*/

        val nonEmptyOutputNodes = nodes.filter(_.outputs.nonEmpty)
        nonEmptyOutputNodes.foreach {
          n =>
            val currFlow = flowMap(n.index)

            val inputs = n.inputs.map(i => flowMap(i.index))
            if (inputs.isEmpty) {
              builder.add(source) ~> currFlow
            } else {
              if (inputs.length > 1) {
                val merge = builder.add(Merge[Seq[ComponentResult]](inputs.length))
                inputs.indices.foreach {
                  idx =>
                    inputs(idx) ~> merge.in(idx)
                }
                merge.out ~> currFlow
              }
            }

            val outputs = n.outputs.map(o => flowMap(o.index))
            /*if (outputs.isEmpty) {
              currFlow ~> snk
            } else {*/
            if (outputs.length > 1) {
              val broadcast = builder.add(Broadcast[Seq[ComponentResult]](outputs.length))
              currFlow ~> broadcast
              outputs.indices.foreach {
                idx =>
                  broadcast.out(idx) ~> outputs(idx)
              }
            } else {
              //todo 这里有问题 outputs都是一个 所以output节点 [Map.in] is already connected，需要改逻辑计划
              if (outputs.nonEmpty)
                currFlow ~> outputs.head
            }
          //}
        }

        //generate final merge
        val emptyOutputlNodes = nodes.filter(_.outputs.isEmpty)
        if (emptyOutputlNodes.length > 1){
          val finalMerge = builder.add(Merge[Seq[ComponentResult]](emptyOutputlNodes.length))
          emptyOutputlNodes.indices.foreach {
            idx =>
              val n = emptyOutputlNodes(idx)
              val currFlow = flowMap(n.index)
              val inputs = n.inputs.map(i => flowMap(i.index))
              if (inputs.length > 1) {
                val merge = builder.add(Merge[Seq[ComponentResult]](inputs.length))
                inputs.indices.foreach {
                  idx =>
                    inputs(idx) ~> merge.in(idx)
                }
                merge.out ~> currFlow
              }

              currFlow ~> finalMerge.in(idx)
          }
          finalMerge.out ~> sink
        } else {
          val n = emptyOutputlNodes.head
          val currFlow = flowMap(n.index)
          val inputs = n.inputs.map(i => flowMap(i.index))
          if (inputs.length > 1) {
            val merge = builder.add(Merge[Seq[ComponentResult]](inputs.length))
            inputs.indices.foreach {
              idx =>
                inputs(idx) ~> merge.in(idx)
            }
            merge.out ~> currFlow
          }
          currFlow ~> sink
        }

        ClosedShape
    })

    //g.run(materializer)
  }

  def run(sc: SparkContext, nodes: Seq[GraphNode[NodeProp]]): NotUsed = {
    val g = parse(sc, nodes)
    g.run(materializer)
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
        val res = nodeType match {
          case NodeType.assembly =>
            /*val jarName = structConf.getString("jar-name")
            val className = structConf.getString("class-name")
            val assembly = ComponentLoader.load(jarName, className)
            val lastRdd = lastResult.head.result
            //执行assembly，获得结果
            // todo 这里的lastRdd可以是个Map结构，适应merge的情况
            val Some(currResult) = assembly.run(sc, paramConf, lastRdd)
            if (!currResult.succeed) {
              throw FlowRunFailedException(s"$nodeType [$index-$name] run failed, message: \n${currResult.message}")
            } else Seq(currResult)*/
            Seq(ComponentResult(succeed = true, s"$nodeType [$index-$name]", None))
          case _ =>
            Seq(ComponentResult(succeed = true, s"$nodeType [$index-$name]", None))
        }
        println(s"Finished $nodeType [$index-$name]")
        res
    }
  }
}
