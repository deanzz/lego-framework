package cn.jw.rms.data.framework.common.utils

import scalaj.http.Http

/**
  * Created by deanzhang on 16/4/13.
  */
object HttpClient {

  def postJson(url: String, json: String) = {
    Http(url).postData(json).header("content-type", "application/json").asString
  }

  def postForm(url: String, formData: String) = {
    Http(url).postData(formData).asString
  }

  /*def main(args: Array[String]): Unit ={
    val url = "http://42.62.78.140:8002/WarningEmail/"
    val json =
      """
        |{
        | "email_receiver":"xnzhang@jointwisdom.cn",
        | "email_subject":"test warning email API1",
        | "email_content":"test warning email API2"
        |}
      """.stripMargin
    val resp = HttpClient.postJson(url, json)
    println("resp = " + resp.body)
  }*/

}
