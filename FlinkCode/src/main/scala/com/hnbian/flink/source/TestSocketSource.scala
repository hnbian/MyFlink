package com.hnbian.flink.source

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2021/1/21 20:29 
  **/
object TestSocketSource extends App {
  // 创建执行环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val stream1: DataStream[String] = env.socketTextStream("localhost",9999)
  stream1.print("TestSocketSource")
  env.execute()
}
