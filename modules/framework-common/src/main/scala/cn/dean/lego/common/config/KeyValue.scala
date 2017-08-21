package cn.dean.lego.common.config

import com.typesafe.config.Config

/**
  * Created by deanzhang on 2016/12/22.
  */
case class KeyValue(key: String, value: String)

object KeyValue{
  def apply(params: Config): KeyValue = {
    KeyValue(
      params.getString("key"),
      params.getString("value")
    )
  }
}
