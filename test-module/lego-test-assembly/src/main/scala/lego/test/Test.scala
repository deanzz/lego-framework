package lego.test

import java.io.File

import cn.dean.lego.common.utils.TimerMeter
import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by deanzhang on 16/2/25.
  */
object Test {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("RmsDataFramework")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").setMaster("local")

    val file = new File("conf/application.conf")
    println("config file path = " + file.getAbsolutePath)
    val conf = ConfigFactory.parseFile(file)

    val sc = new SparkContext(sparkConf)
    val cleaner = new Cleaner
    val start = TimerMeter.start("lego test")
    cleaner.clean(sc, conf, None)
    cleaner.clean(sc, conf, None)
    val ms = TimerMeter.end("lego test", start)
    println (s"Elasped Time = $ms s")

   /* val regex = """^.*\d{4}-\d{2}-\d{2}$"""
    val str = "asdf2016-10-10"
    println(str.matches(regex))*/
  }

}

//spark-submit --class cn.jw.rms.ab.pred.shortterm.hourly.Test --master local[2] --driver-memory 4G --conf spark.shuffle.consolidateFiles=true --conf spark.rdd.compress=true ./rms-ab-micro-pred-shortterm-hourly-assembly-1.3.jar