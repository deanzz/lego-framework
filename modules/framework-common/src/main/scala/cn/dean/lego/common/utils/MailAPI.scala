package cn.dean.lego.common.utils

import scalaj.http.HttpResponse

/**
  * Created by deanzhang on 2016/12/27.
  */
object MailAPI {
   def sendMail(url: String, subject: String, body: String, mailTo: Seq[String]): HttpResponse[String] = {
    val jsonTpl =
      """
        |{
        | "email_receiver":"%s",
        | "email_subject":"%s",
        | "email_content":"%s"
        |}
      """.stripMargin
    val mailToStr = mailTo.mkString(",")
    val json = jsonTpl.format(mailToStr, subject, body)
    HttpClient.postJson(url, json)
  }
}
