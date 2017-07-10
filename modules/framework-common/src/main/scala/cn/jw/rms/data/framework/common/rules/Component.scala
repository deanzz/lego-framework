package cn.jw.rms.data.framework.common.rules

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by deanzhang on 2016/12/22.
  */
trait Component {

  def run(sc: SparkContext,
          conf: Option[Config] = None,
          prevStepRDD: Option[RDD[String]] = None): Option[AssemblyResult] = throw new UnsupportedOperationException

}
