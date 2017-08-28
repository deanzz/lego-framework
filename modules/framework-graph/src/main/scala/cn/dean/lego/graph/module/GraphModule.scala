package cn.dean.lego.graph.module

import akka.actor.ActorSystem
import scaldi.Module
/**
  * Created by deanzhang on 2017/8/25.
  */
class GraphModule extends Module{

  bind[ActorSystem] to ActorSystem("lego-framework") destroyWith (_.terminate())

}
