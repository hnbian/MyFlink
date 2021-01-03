package com.hnbian.flink.window

import com.hnbian.flink.common.{MinDataReduceFunction, Obj1}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows

/**
  * @Author haonan.bian
  * @Description 会话窗口
  * @Date 2020/8/15 23:43 
  **/
object TestSessionProcessingTimeSessionWindow {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val stream1: DataStream[String] = env.socketTextStream("localhost",9999)

    stream1.print()
    val stream2: DataStream[Obj1] = stream1.map(data => {
      val arr = data.split(",")
      Obj1(arr(0), arr(1), arr(2).toLong)
    })

    stream2.keyBy(0)
      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
      .reduce(new MinDataReduceFunction)
      .print("ProcessingTimeSessionWindows")

    env.execute()
  }
}
