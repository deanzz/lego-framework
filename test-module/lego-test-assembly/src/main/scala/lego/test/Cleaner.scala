package lego.test

import java.io.Serializable

import cn.dean.lego.common.rules.CleanerAssembly
import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.immutable.Map

import scala.util.{Failure, Success, Try}

/**
  * Created by deanzhang on 16/2/25.
  */

object SeriLogger extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
}

class Cleaner extends CleanerAssembly with Serializable {

  import SeriLogger.logger
  var gotSucceed = false
  var errMsg = ""

  override def succeed: (Boolean, String) = (gotSucceed, errMsg)

  override def clean(sc: SparkContext, config: Config, prevStepRDD: Option[Map[String,RDD[String]]]): Option[RDD[String]] = {
    Try {
      println("lego test started")
      println(s"prevStepRDD:\n$prevStepRDD")
      val throwException = config.getBoolean("throw-exception")
      val fcDate = config.getString("fc-date")
      val testEnable = config.getBoolean("test-enable")
      val fcDays = config.getInt("fc-days")
      val returnRdd = Try(config.getBoolean("return-rdd")).getOrElse(false)
      val name = config.getString("name")

      println(s"fcDate = $fcDate")
      println(s"testEnable = $testEnable")
      println(s"fcDays = $fcDays")
      println(s"return-rdd = $returnRdd")

      if(throwException)
        throw new Exception("Throw a test exception")
      else {
        errMsg = if (prevStepRDD.isDefined && prevStepRDD.get.nonEmpty)
          prevStepRDD.get.values.reduce(_ union _).collect().mkString("#")
        else "None"
      }

      println(errMsg)
      println(s"1:sc.isStopped = ${sc.isStopped}")
      if(returnRdd){
        val seq = Seq("a","b","c")
        println(s"2:sc.isStopped = ${sc.isStopped}")
        val rdd = sc.makeRDD(seq)
        println(s"3:sc.isStopped = ${sc.isStopped}")
        val newRdd = if(prevStepRDD.isDefined && prevStepRDD.get.nonEmpty) {
          println(s"prevStepRDD.get.keys = ${prevStepRDD.get.keys.mkString(", ")}")
          prevStepRDD.get.values.reduce(_ union _).union(rdd)
        } else rdd
        Some(newRdd)
      } else None
    } match {
      case Success(res) =>
        gotSucceed = true
        res
      case Failure(e) =>
        gotSucceed = false
        errMsg = e.toString
        logger.error(errMsg)
        println(errMsg)
        e.printStackTrace()
        None
    }
  }
}
