package com.hnbian.flink.source

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * @Author haonan.bian
  * @Description 测试从文件中读取数据
  * @Date 2020-07-11 10:37 
  **/
object SourceTestFile {

  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.readTextFile("/opt/apache-maven-3.6.0/conf/settings.xml")

    stream.print()

    env.execute()


  }
}

















