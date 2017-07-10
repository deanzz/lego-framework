package cn.jw.rms.data.framework.core.config

import com.typesafe.config.Config

/**
  * Created by deanzhang on 2016/12/22.
  */
case class PartConf(name: String, dir: String, index: Int, enable: Boolean)

object PartConf{
  def apply(params: Config): PartConf = {
    PartConf(params.getString("name"),
      params.getString("dir"),
      params.getInt("index"),
      params.getBoolean("enable")
    )
  }
}