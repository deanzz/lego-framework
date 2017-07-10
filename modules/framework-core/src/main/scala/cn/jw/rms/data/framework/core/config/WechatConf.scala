package cn.jw.rms.data.framework.core.config

import com.typesafe.config.Config

/**
  * Created by deanzhang on 2017/4/7.
  */

case class WechatConf(apiUrl: String, app: String, group: String, enable: Boolean)

object WechatConf{
  def apply(params: Config): WechatConf = {
    WechatConf(
      params.getString("api.url"),
      params.getString("app"),
      params.getString("group"),
      params.getBoolean("enable")
    )
  }
}