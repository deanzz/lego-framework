package cn.dean.lego.graph.physicalplan

import java.net.InetAddress

import akka.actor.Actor
import cn.dean.lego.common.{TYPE_APPLICATION, TYPE_MODULE, TYPE_SYSTEM}
import cn.dean.lego.common.config.{MailConf, WechatConf}
import cn.dean.lego.common.log.Logger
import cn.dean.lego.common.rules.ComponentResult
import cn.dean.lego.common.utils.{MailAPI, WechatAPI}
import com.typesafe.config.Config
import scaldi.Injectable.inject
import scaldi.Injector
import scala.util.Try

/**
  * Created by deanzhang on 2017/8/22.
  */
class NotifyActor(implicit injector: Injector) extends Actor {
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

  //应用类型，system, application or module
  private val confType = conf.getString("type")
  private val componentName = confType match {
    case TYPE_SYSTEM => s"System[$appName]"
    case TYPE_APPLICATION => s"Application[$appName]"
    case TYPE_MODULE => s"Module[$appName]"
    case _ => s"Unknown[$appName]"
  }

  override def receive: Receive = {
    case r: ComponentResult =>
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

      logger.info("Terminate actor system...")
      context.system.terminate()
  }
}
