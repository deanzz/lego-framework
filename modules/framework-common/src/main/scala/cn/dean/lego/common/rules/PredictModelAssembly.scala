package cn.dean.lego.common.rules

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.immutable.Map

/**
  * Created by deanzhang on 15/12/29.
  */
trait PredictModelAssembly extends Component{
  /**
    *
    * @param sc SparkContext object on framework
    * @param config Config object, can get assembly parameters by config
    * @param prevStepRDD Data set RDD[String] of previous job
    * @return Data set RDD[String] of current job
    */
  def accuracy(sc: SparkContext, config: Config,
               prevStepRDD: Option[Map[String, RDD[String]]] = None ): String
  /**
    *
    * @param sc SparkContext object on framework
    * @param config Config object, can get assembly parameters by config
    * @param prevStepRDD Data set RDD[String] of previous job
    * @return Data set RDD[String] of current job
    */
  def predict(sc: SparkContext, config: Config,
              prevStepRDD: Option[Map[String, RDD[String]]] = None): Option[RDD[String]]
  /***
    * If job succeed
    * @return (Boolean, String), 1st: will be true or false, true means succeed, false means failed; 2nd: will be "" if 1st is true, will be error message if 1st is false
    */
  def succeed: (Boolean, String)

  override def run(sc: SparkContext, config: Option[Config] = None,
                   prevStepRDD: Option[Map[String, RDD[String]]] = None): Option[ComponentResult] = {
    val resultOpt = predict(sc, config.get, prevStepRDD)
    val (succd, message) = succeed
    val name = config.get.getString("name")
    val result = resultOpt.map(r => Option(Map(name -> r))).getOrElse(None)
    Some(ComponentResult(name, succd, message, result))
  }

}
