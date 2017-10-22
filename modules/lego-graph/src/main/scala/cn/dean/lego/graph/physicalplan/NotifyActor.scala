package cn.dean.lego.graph.physicalplan

import java.net.InetAddress

import akka.actor.Actor
import cn.dean.lego.common.{TYPE_APPLICATION, TYPE_MODULE, TYPE_SYSTEM}
import cn.dean.lego.common.config.{MailConf, WechatConf}
import cn.dean.lego.common.log.Logger
import cn.dean.lego.common.rules.ComponentResult
import cn.dean.lego.common.utils.{MailAPI, WechatAPI}
import cn.dean.lego.graph.physicalplan.NotifyActor.{AddResultLog, PlanStart}
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.joda.time.DateTime
import scaldi.Injector
import scaldi.akka.AkkaInjectable

import scala.collection.mutable.ListBuffer
import scala.util.Try

/**
  * Created by deanzhang on 2017/8/22.
  */
class NotifyActor(implicit injector: Injector) extends Actor with AkkaInjectable {
  private val conf = inject[Config]
  private val logger = inject[Logger]
  //获取邮件配置
  private val mailConf = MailConf(conf.getConfig("mail"))
  //获取微信配置
  private val wechatConf = Try(WechatConf(conf.getConfig("wechat"))).getOrElse(WechatConf("", "", "", enable = false))

  //获取客户端基础信息
  private val localhost = InetAddress.getLocalHost.getHostAddress
  private val currentUser = System.getProperty("user.name")
  private val serverInfo = s"$currentUser@$localhost"

  //应用名称
  private val appName = conf.getString("name")
  private var startedAt: DateTime = _
  private val assemblyResults = ListBuffer.empty[String]

  //应用类型，system, application or module
  private val confType = conf.getString("type")
  private val componentName = confType match {
    case TYPE_SYSTEM => s"System[$appName]"
    case TYPE_APPLICATION => s"Application[$appName]"
    case TYPE_MODULE => s"Module[$appName]"
    case _ => s"Unknown[$appName]"
  }

  override def receive: Receive = {
    case r: Seq[ComponentResult] =>
      logger.info(s"NotifyActor.r = $r")
      val succeed = r.forall(_.succeed)
      try {
        val subject =
          if (succeed) {
            s"[${mailConf.subjectPrefix}][$serverInfo] $componentName execute succeed"
          } else {
            s"[${mailConf.subjectPrefix}][WARN!!][$serverInfo] $componentName execute failed"
          }
        val now = DateTime.now
        val body = s"$componentName start at ${startedAt.toString("yyyy-MM-dd HH:mm:ss")}, finished at ${now.toString("yyyy-MM-dd HH:mm:ss")}, total elapsed time = ${now.getMillis - startedAt.getMillis}ms.\n${assemblyResults.mkString("\n")}"
        logger.info(s"mail body:\n$body")
        //send mail
        val mailResp = MailAPI.sendMail(mailConf.apiUrl, subject, body, mailConf.toList)
        logger.info(s"The resp of sending mail [$subject] is [$mailResp]")
        //send wechat message
        if (wechatConf.enable) {
          val wechatResp = WechatAPI.send(wechatConf.apiUrl, wechatConf.group, wechatConf.app, componentName, serverInfo, body, succeed)
          logger.info(s"The resp of sending wechat [$subject] is [$wechatResp]")
        }
      } catch {
        case e: Exception =>
          val lstTrace = e.getStackTrace.map(_.toString).mkString("\n")
          val err = s"${e.toString}\n$lstTrace"
          logger.error(err)
      } finally {
        logger.info("Stop SparkContext...")
        inject[SparkContext].stop()
        logger.info("Terminate actor system...")
        context.system.terminate()
      }

    case PlanStart(startTime) =>
      startedAt = startTime

    case AddResultLog(log) =>
      assemblyResults += log

    case unknown => logger.error(s"PhysicalRunner got unknown message [$unknown]")
  }
}

object NotifyActor {

  case class PlanStart(startTime: DateTime)

  case class AddResultLog(log: String)

}


