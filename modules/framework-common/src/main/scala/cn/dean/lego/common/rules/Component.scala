package cn.dean.lego.common.rules

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by deanzhang on 2016/12/22.
  */
trait Component {

  def run(sc: SparkContext,
          conf: Option[Config] = None,
          prevStepRDD: Option[RDD[String]] = None): Option[ComponentResult] = throw new UnsupportedOperationException

 /* def run(sc: SparkContext,
          conf: Option[Config] = None,
          prevStepRDDList: Seq[RDD[String]]): Option[ComponentResult] = throw new UnsupportedOperationException

  /***
    * If job succeed
    * @return (Boolean, String), 1st: will be true or false, true means succeed, false means failed; 2nd: will be "" if 1st is true, will be error message if 1st is false
    */
  def succeed: (Boolean, String)*/
}
