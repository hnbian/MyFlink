package com.hnbian.flink.state

import org.apache.flink.streaming.api.scala._

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2021/1/19 22:43 
  **/
object TestValueState extends App {

  private val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  val stream1: DataStream[String] = environment.socketTextStream("localhost", 9999)





}
