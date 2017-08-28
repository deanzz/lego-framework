package cn.dean.lego.core

import cn.dean.lego.common.rules.{ComponentResult, Component}
import cn.dean.lego.common.utils.TimerMeter
import cn.dean.lego.common.config._
import cn.dean.lego.common.exceptions.PartNotFoundException
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.reflect.io.Path

/**
  * Created by deanzhang on 2016/12/22.
  */

/**
  * System级别应用，根据配置加载其下的各Application并执行
  * @param config 配置信息
  */
class System(config: Config) extends Component{

  //获取全局上下文参数
  private[this] val contextParameters: Seq[KeyValue] = config.getConfigList("context.parameters").map(KeyValue.apply)
  //获取System下的各application的配置
  private[this] val partConfList: Seq[PartConf] = config.getConfigList("parts").map(PartConf.apply).filter(_.enable).sortBy(_.index)

  /**
    * System的运行函数，只可被main函数启动
    * @param sc 当前框架中的SparkContext
    * @param conf 当前System的配置内容
    * @param prevStepRDD 之前assembly的执行结果RDD，这里为空，不需要
    * @return 返回System的执行结果
    */
  override def run(sc: SparkContext, conf: Option[Config] = None,
                   prevStepRDD: Option[RDD[String]] = None): Option[ComponentResult] = {
    var result: Option[ComponentResult] = None
    import scala.util.control.Breaks.{break, breakable}
    val mailBody = new StringBuilder
    breakable {
      //遍历application配置列表，一个app一个app加载并启动
      for(p <- partConfList) {
        val partName = p.name
        val timerName = s"application:$partName"
        val start = TimerMeter.start(timerName)
        val partDir = p.dir
        val part = loadPart(sc, partName, partDir)
        val currResultOpt = part.run(sc)
        result = currResultOpt
        val elaspedTime = TimerMeter.end(timerName, start)
        //如果当前app执行结果不为空
        if(currResultOpt.isDefined){
          val currResult = currResultOpt.get
          //如果当前app执行结果为失败
          if (!currResult.succeed) {
            mailBody.append(s"Application[$partName] execute !!FAILED!!, Elasped Time = ${elaspedTime}s\n${currResult.message}")
            break()
            //如果当前app执行结果为成功
          } else{
            mailBody.append(s"Application[$partName] execute succeed, Elasped Time = ${elaspedTime}s\n${currResult.message}")
          }
          //如果当前app执行结果为空
        } else break()
      }
    }

    if(result.isDefined) {
      val body = mailBody.toString()
      Some(result.get.copy(message = body))
    } else result
  }

  /**
    * 根据配置加载一个Application
    * @param sc 当前框架中的SparkContext
    * @param name application名称
    * @param dir app所在目录，可以是local或hdfs
    * @return 返回一个Application实例
    */
  private def loadPart(sc: SparkContext, name: String, dir: String) = {
    val path: Path = Path(dir)
    if(!path.exists){
      throw PartNotFoundException(s"Application [$name][$dir] not found")
    }
    val confPath = s"$dir/conf/application.conf"
    val conf = ConfigLoader.load(sc, confPath)
    new Application(conf, Some(contextParameters))
  }

}
