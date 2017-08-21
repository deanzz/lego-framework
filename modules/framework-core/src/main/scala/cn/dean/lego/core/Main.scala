package cn.dean.lego.core

import java.net.InetAddress

import cn.dean.lego.common.log.Logger
import cn.dean.lego.common.rules.AssemblyResult
import cn.dean.lego.common.utils.{MailAPI, TimerMeter, WechatAPI}
import cn.dean.lego.common.config.{ConfigLoader, MailConf, WechatConf}
import org.apache.spark.{SparkConf, SparkContext}
import cn.dean.lego.common._

import scala.util.{Failure, Success, Try}

/**
  * Created by deanzhang on 15/11/27.
  */
object Main {

  /**
    * 入口函数
    * @param args 入口函数参数，目前有一个参数，配置文件绝对路径（local/hdfs）
    */
  def main(args: Array[String]): Unit = {

    //获取配置文件路径
    val configPath =
      if (args.length == 1)
        args(0)
      else {
       //logger.warn("No specify configuration file path. Using default configuration file path")
        "conf/application.conf"
      }

    //获取SparkConf
    val sparkConf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //sparkConf.set("io.compression.codecs", "com.hadoop.compression.lzo.LzopCodec")
    //获取SparkContext
    val sc = SparkContext.getOrCreate(sparkConf)
    //加载配置信息
    val conf = ConfigLoader.load(sc, configPath)
    //应用类型，system, application or module
    val confType = conf.getString("type")
    //应用名称
    val appName = conf.getString("name")
    //日志目录，hdfs目录必须以hdfs://开头
    val logDir = conf.getString("log.dir")
    //日志对象
    val logger = Logger(logDir)
    //获取客户端基础信息
    val localhost = InetAddress.getLocalHost.getHostAddress
    val currentUser = System.getProperty("user.name")
    val serverInfo = s"$currentUser@$localhost"
    logger.info(s"serverInfo = $serverInfo")

    val componentName = confType match {
      case TYPE_SYSTEM => s"System[$appName]"
      case TYPE_APPLICATION => s"Application[$appName]"
      case TYPE_MODULE => s"Module[$appName]"
      case _ => s"Unknown[$appName]"
    }

    //获取邮件配置
    val mailConf = MailConf(conf.getConfig("mail"))
    //获取微信配置
    val wechatConf = Try(WechatConf(conf.getConfig("wechat"))) match {
      case Success(r) => r
      case Failure(e) => WechatConf("", "", "", enable = false)
    }

    Try {
      val timerStart = TimerMeter.start(componentName)
      //根据不同应用类型，启动不同级别的任务
      val result = confType match {
        case TYPE_SYSTEM =>
          val system = new System(conf)
          system.run(sc)
        case TYPE_APPLICATION =>
          val application = new Application(conf)
          application.run(sc)
        case TYPE_MODULE =>
          val module = new Module(conf)
          module.run(sc)
        case _ =>
          throw new UnsupportedOperationException(s"Unsupported type [$confType], only support [$TYPE_SYSTEM, $TYPE_APPLICATION, $TYPE_MODULE]")
      }
      val elaspedTime = TimerMeter.end(componentName, timerStart)
      //获取任务执行结果，成功或失败（包含失败原因）
      if (result.isDefined) {
        val origMsg = result.get.message
        result.get.copy(message = s"Total Elasped Time = ${elaspedTime}s\n$origMsg")
      } else
        AssemblyResult(succeed = false, "No assembly enabled, please check the configuration [assemblies -> enable] under application.conf of module", None)
    } match {
        //任务执行成功处理
      case Success(r) =>
        val subject =
          if (r.succeed) {
            val str = s"[${mailConf.subjectPrefix}][$serverInfo] $componentName execute succeed"
            logger.info(r.message)
            str
          } else {
            val str = s"[${mailConf.subjectPrefix}][WARN!!][$serverInfo] $componentName execute failed"
            logger.error(r.message)
            str
          }
        //send mail
        val mailResp = MailAPI.sendMail(mailConf.apiUrl, subject, r.message, mailConf.toList)
        logger.info(s"The resp of sending mail [$subject] is [$mailResp]")
        //send wechat message
        if (wechatConf.enable) {
          val wechatResp = WechatAPI.send(wechatConf.apiUrl, wechatConf.group, wechatConf.app, componentName, serverInfo, r.message, r.succeed)
          logger.info(s"The resp of sending wechat [$subject] is [$wechatResp]")
        }
      //任务执行失败处理
      case Failure(e) =>
        val lstTrace = e.getStackTrace.map(_.toString).mkString("\n")
        val err = s"${e.toString}\n$lstTrace"
        logger.error(err)
        val subject = s"[${mailConf.subjectPrefix}][WARN!!][$serverInfo] $componentName execute failed"
        e.printStackTrace()
        val resp = MailAPI.sendMail(mailConf.apiUrl, subject, err, mailConf.toList)
        logger.info(s"The resp of sending mail [$subject] is [$resp]")
        //send wechat message
        if (wechatConf.enable) {
          val wechatResp = WechatAPI.send(wechatConf.apiUrl, wechatConf.group, wechatConf.app, componentName, serverInfo, err, succeed = false)
          logger.info(s"The resp of sending wechat [$subject] is [$wechatResp]")
        }
    }
  }
}
