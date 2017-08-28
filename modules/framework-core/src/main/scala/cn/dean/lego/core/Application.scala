package cn.dean.lego.core

import cn.dean.lego.common.rules.{ComponentResult, Component}
import cn.dean.lego.common.utils.TimerMeter
import cn.dean.lego.common.config._
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.io.Source

/**
  * Created by deanzhang on 2016/12/22.
  */

/**
  * Application级别应用，根据配置加载其下的各module并执行
  * @param config 配置信息
  * @param contextParametersOpt 上游传下来的全局上下文参数
  */
class Application(config: Config,
                  contextParametersOpt: Option[Seq[KeyValue]] = None) extends Component {

  //获取全局上下文参数
  private[this] val contextParameters: Seq[KeyValue] =
    if (contextParametersOpt.isDefined)
      contextParametersOpt.get
    else
      config.getConfigList("context.parameters").map(KeyValue.apply)

  //获取Application下的各module的配置
  private[this] val moduleConfList: Seq[PartConf] = config.getConfigList("parts").map(PartConf.apply).filter(_.enable).sortBy(_.index)

  /**
    * Application的运行函数，可被main函数启动或被上级system启动
    * @param sc 当前框架中的SparkContext
    * @param conf 当前Application的配置内容
    * @param prevStepRDD 之前assembly的执行结果RDD，这里为空，不需要
    * @return 返回Application执行结果
    */
  override def run(sc: SparkContext, conf: Option[Config] = None,
                   prevStepRDD: Option[RDD[String]] = None): Option[ComponentResult] = {
    var result: Option[ComponentResult] = None
    import scala.util.control.Breaks.{break, breakable}
    val mailBody = new StringBuilder
    breakable {
      //遍历module配置列表，一个module一个module加载并启动
      for (mc <- moduleConfList) {
        val moduleName = mc.name
        val timerName = s"module:$moduleName"
        val start = TimerMeter.start(timerName)
        val moduleDir = mc.dir
        val module = loadModule(sc, moduleDir)
        val currResultOpt = module.run(sc)
        result = currResultOpt
        val elaspedTime = TimerMeter.end(timerName, start)
        //如果当前module执行结果不为空
        if(currResultOpt.isDefined){
          val currResult = currResultOpt.get
          //如果当前module执行结果为失败
          if (!currResult.succeed) {
            mailBody.append(s"    Module[$moduleName] execute !!FAILED!!, Elasped Time = ${elaspedTime}s\n${currResult.message}")
            break()
            //如果当前module执行结果为成功
          } else {
            mailBody.append(s"    Module[$moduleName] execute succeed, Elasped Time = ${elaspedTime}s\n${currResult.message}")
          }
          //如果当前module执行结果为空
        } else break()
      }
    }
    if(result.isDefined) {
      val body = mailBody.toString()
      Some(result.get.copy(message = body))
    } else result
  }

  /**
    * 根据配置加载一个module
    * @param sc 当前框架中的SparkContext
    * @param dir module所在目录，可以是local或hdfs
    * @return 返回一个Module实例
    */
  private def loadModule(sc: SparkContext, dir: String) = {
    val isHdfs = dir.trim.toLowerCase.startsWith("hdfs://")
    //读取指定module目录中的配置文件
    val confPath = s"$dir/conf/application.conf"
    var confContent =
      if(isHdfs) sc.textFile(confPath).collect().mkString("\n")
      else Source.fromFile(confPath).getLines().mkString("\n")

    //将全局参数替换到每个模块的配置文件中
    contextParameters.foreach {
      case KeyValue(k, v) =>
        val origLine =
          s"""[ 	]+$k\\s*=\\s*.+"""
        //println(s"reg = $origLine")
        val newLine =
          s"""    $k = "$v""""
        confContent = confContent.replaceAll(origLine, newLine)
    }

    val conf = ConfigLoader.load(confContent)
    new Module(conf)
  }

}
