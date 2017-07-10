package cn.jw.rms.data.framework.core

import java.io.File
//import java.net.{URL, URLClassLoader}

import cn.jw.rms.data.framework.common.rules.Component
//import cn.jw.rms.data.framework.core.classloader.HdfsClassLoader
//import org.apache.hadoop.fs.Path

/**
  * Created by deanzhang on 2016/12/26.
  */

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

  /*val hadoopConf = new org.apache.hadoop.conf.Configuration()
  def load(jarPath: String, clsName: String): Component = {
    val classLoader = if(jarPath.trim.toLowerCase.startsWith("hdfs://")){
      new HdfsClassLoader(hadoopConf, new Path(jarPath))
    } else {
      val arrUrl = Array(new File(jarPath).toURI.toURL)
      new java.net.URLClassLoader(
        arrUrl,
        /*
         * need to specify parent, so we have all class instances
         * in current context
         */
        this.getClass.getClassLoader)
    }

    val clazzExModule = classLoader.loadClass(clsName)
    clazzExModule.newInstance().asInstanceOf[Component]
  }*/

}

//https://stackoverflow.com/questions/29444582/custom-classloader-in-java-for-loading-jar-from-hdfs
