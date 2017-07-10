package cn.jw.rms.data.framework.common.utils

import java.net.URLEncoder
import org.joda.time.DateTime
import scalaj.http.HttpResponse

/**
  * Created by deanzhang on 2017/4/7.
  */
object WechatAPI {
  def send(url: String, group: String, app: String, componentName: String, serverInfo: String, body: String, succeed: Boolean): HttpResponse[String] = {
    val status = if(succeed) "成功" else "失败"
    val statusColor = if(succeed) "#4DB361" else "#BB4444"
    val now = s"${DateTime.now().toString("yyyy-MM-dd HH:mm:ss")}"
    val dataTpl = """{"first":{"value":"%s","color":"#000"},"keyword1":{"value":"%s","color":"#173177"},"keyword2":{"value":"%s","color":"#173177"},"keyword3":{"value":"%s","color":"%s"},"remark":{"value":"%s","color":"#173177"}}"""
    val data = dataTpl.format(now, serverInfo, componentName, status, statusColor, body.replace("\n","\\n"))
    val encodedData = URLEncoder.encode(data, "UTF-8")
    val formData = s"appid=$app&group=$group&data=$encodedData"
    HttpClient.postForm(url, formData)
  }
}
