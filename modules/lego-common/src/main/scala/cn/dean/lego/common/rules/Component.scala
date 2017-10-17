package cn.dean.lego.common.rules

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.immutable.Map

/**
  * Created by deanzhang on 2016/12/22.
  */
trait Component {

  def run(sc: SparkContext,
          conf: Option[Config] = None,
          prevStepRDD: Option[Map[String, RDD[String]]] = None): Option[ComponentResult] = throw new UnsupportedOperationException

}
