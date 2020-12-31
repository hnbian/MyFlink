package com.hnbian.flink.window

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.triggers.{CountTrigger, EventTimeTrigger}
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner
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
      .trigger(CountTrigger.of(3)) // 当处理 3 个或 3 的倍数个事件触发
      .sum(1)
      .print()

    /**
      * 11> 1
      * 12> 2
      * 1> 3
      * 11> (1,3) -- 第一次触发
      * 2> 4
      * 3> 5
      * 4> 6
      * 12> (1,6) -- 第二次触发
      * 5> 7
      */

    env.execute()
  }
}





