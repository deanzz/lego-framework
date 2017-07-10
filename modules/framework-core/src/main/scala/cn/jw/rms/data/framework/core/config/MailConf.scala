package cn.jw.rms.data.framework.core.config

import com.typesafe.config.Config
import collection.JavaConversions._

/**
  * Created by deanzhang on 2016/12/22.
  */
case class MailConf(apiUrl: String, toList: Seq[String], subjectPrefix: String)

object MailConf{
  def apply(params: Config): MailConf = {
    MailConf(
      params.getString("api.url"),
      params.getStringList("to"),
      params.getString("subject-prefix")
    )
  }
}
