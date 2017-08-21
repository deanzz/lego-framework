package cn.dean.lego.common.config

import com.typesafe.config.Config

/**
  * Created by deanzhang on 2016/12/22.
  */
case class PartConf(name: String, dir: String, index: String, enable: Boolean)

object PartConf{
  def apply(params: Config): PartConf = {
    PartConf(params.getString("name"),
      params.getString("dir"),
      params.getString("index"),
      params.getBoolean("enable")
    )
  }
}