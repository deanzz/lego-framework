package cn.dean.lego.graph.module

import akka.actor.{ActorSystem, Props}
import cn.dean.lego.common.config.ConfigLoader
import cn.dean.lego.common.log.Logger
import cn.dean.lego.graph.logicplan.TypesafeConfigLogicalParser
import cn.dean.lego.graph.physicalplan.{AkkaPhysicalParser, NotifyActor}
import com.typesafe.config.Config
import org.apache.spark.{SparkConf, SparkContext}
import scaldi.Module
import scaldi.akka.AkkaInjectable

/**
  * Created by deanzhang on 2017/8/25.
  */
class GraphModule(configPath: String) extends Module {

  bind[SparkConf] to new SparkConf()
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .setMaster("local")
    .setAppName("legoV3")

  bind[SparkContext] to SparkContext.getOrCreate(inject[SparkConf])

  bind[Config] to ConfigLoader.load(inject[SparkContext], configPath)

  bind[Logger] to new Logger

  bind[ActorSystem] to ActorSystem("lego-framework", inject[Config]) destroyWith (_.terminate())

  binding identifiedBy 'notifyActor to {
    implicit val actorSystem: ActorSystem = inject[ActorSystem]
    actorSystem.actorOf(Props[NotifyActor], "notifyActor")
  }

  bind[TypesafeConfigLogicalParser] to new TypesafeConfigLogicalParser

  bind[AkkaPhysicalParser] to new AkkaPhysicalParser

}
