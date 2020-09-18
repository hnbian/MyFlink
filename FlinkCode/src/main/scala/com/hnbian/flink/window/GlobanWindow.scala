package com.hnbian.flink.window

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2020/9/17 14:18 
  **/
object GlobanWindow {

  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream1: DataStream[String] = env.socketTextStream("localhost",9999)

    stream1.print()

    // 当单词累计出现的次数每达到10次时，则触发计算，计算整个窗口内该单词出现的总数
    stream1
      .flatMap(str=>{str.split(" ")})
      .map(str=>{(str,1)})
      .keyBy(0)
      .windowAll(GlobalWindows.create())
      .trigger(CountTrigger.of(3))
      .sum(1)
      .print()

    env.execute()
  }
}