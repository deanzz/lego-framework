package cn.dean.lego.common.test

import cn.dean.lego.common.config.ConfigLoader
import org.scalatest.FlatSpec

/**
  * Created by deanzhang on 2017/8/20.
  */

class ConfigLoaderSpec extends FlatSpec {

  "ConfigLoader" should "parse path succeed" in {
    val path = "/Users/deanzhang/work/code/github/lego-framework/sample/systemA/appA/moduleA/conf/application.conf"
    val conf = ConfigLoader.load(path, None)
    assert(conf != null)
  }

  /*it should "throw NoSuchElementException if an empty stack is popped" in {
    val emptyStack = new Stack[String]
    assertThrows[NoSuchElementException] {
      emptyStack.pop()
    }
  }*/
}