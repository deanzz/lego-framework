package cn.dean.lego.common.rules

import org.apache.spark.rdd.RDD

import scala.collection.immutable.Map

/**
  * Created by deanzhang on 2016/12/26.
  */
/**
  * assembly执行结果结构体
  * @param succeed 是否执行成功
  * @param message 返回的消息，可以是记录性质信息也可以是错误信息
  * @param result assembly执行返回的RDD
  */
case class ComponentResult(name: String, succeed: Boolean, message: String, result: Option[Map[String, RDD[String]]])