package cn.jw.rms.data.framework.core

import cn.jw.rms.data.framework.common.rules.{AssemblyResult, Component}
import cn.jw.rms.data.framework.common.utils.TimerMeter
import cn.jw.rms.data.framework.core.config.AssemblyConf
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.util.control.Breaks
import scala.collection.JavaConversions._
import cn.jw.rms.data.framework.common.MY_LOG_PRE
import org.joda.time.DateTime

import scala.util.{Failure, Success, Try}

/**
  * Created by deanzhang on 2016/12/22.
  */

/**
  * module级别应用，根据配置加载其下的各assembly并执行
  * @param config 配置信息
  */
class Module(val config: Config) extends Component {

  //获取模块名称
  private[this] val name =  Try(config.getString("name")) match {
    case Success(r) => r
    case Failure(e) => "-"
  }
  //获取所有assembly的jar包配置
  private[this] val assemblies = config.getConfigList("assemblies").map(AssemblyConf.apply).filter(_.enable).sortBy(_.index)
  //获取所有assembly的参数配置
  private[this] val params = config.getConfigList("parameters")



  /**
    * Module的运行函数，可被main函数启动或被上级application启动
    * @param sc 当前框架中的SparkContext
    * @param conf 当前Module的配置内容
    * @param prevStepRDD 之前assembly的执行结果RDD
    * @return 返回Moduel的执行结果
    */
  override def run(sc: SparkContext, conf: Option[Config] = None,
                   prevStepRDD: Option[RDD[String]] = None): Option[AssemblyResult] = {
    //上一步assembly的执行结果RDD
    var lastResOpt: Option[RDD[String]] = prevStepRDD
    var result: Option[AssemblyResult] = None
    val mailBody = new StringBuilder
    import Breaks.{break, breakable}
    breakable {
      mailBody.append(s"    Module[$name] started at ${DateTime.now}\n")
      //遍历assembly配置列表，一个assembly一个assembly加载并启动
      for (c <- assemblies) {
        val timerName = s"assembly:${c.name}"
        println(s"${DateTime.now}  $timerName started")
        val start = TimerMeter.start(timerName)
        //加载jar包并实例化
        val assembly = ComponentLoader.load(s"${c.jarName}"/*s"$assembliesDir/${c.jarName}"*/, c.className)
        //获取对应参数
        val param = params.filter(_.getString("name") == c.name).head
        //执行assembly，获得结果
        val Some(currResult) = assembly.run(sc, Some(param), lastResOpt)
        lastResOpt = currResult.result
        result = Some(currResult)
        val elaspedTime = TimerMeter.end(timerName, start)
        //如果当前assembly执行结果为失败
        if (!currResult.succeed) {
          mailBody.append(s"        Assembly[${c.name}] execute !!FAILED!!, Elasped Time = ${elaspedTime}s\n        $MY_LOG_PRE${currResult.message}\n")
          break()
          //如果当前assembly执行结果为成功
        } else {
          mailBody.append(s"        Assembly[${c.name}] execute succeed, Elasped Time = ${elaspedTime}s\n        $MY_LOG_PRE${currResult.message}\n")
        }
        println(s"${DateTime.now}  $timerName finished")
      }
      mailBody.append(s"    Module[$name] finished at ${DateTime.now}\n")
    }

    if(result.isDefined){
      val body = mailBody.toString()
      Some(result.get.copy(message = body))
    } else result
  }
}
