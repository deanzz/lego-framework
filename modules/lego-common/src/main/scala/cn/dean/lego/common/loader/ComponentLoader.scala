package cn.dean.lego.common.loader

import java.io.File

import cn.dean.lego.common.rules.Component

/**
  * 加载assembly的jar包
  */
object ComponentLoader {
  def load(jarPath: String, clsName: String): Component = {
    val classLoader = new java.net.URLClassLoader(
      Array(new File(jarPath).toURI.toURL),
      /*
       * need to specify parent, so we have all class instances
       * in current context
       */
      this.getClass.getClassLoader)

    val clazzExModule = classLoader.loadClass(clsName)
    clazzExModule.newInstance().asInstanceOf[Component]
  }
}
