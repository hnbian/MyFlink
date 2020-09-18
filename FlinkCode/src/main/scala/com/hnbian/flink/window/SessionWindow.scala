package com.hnbian.flink.window

import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows

/**
  * @Author haonan.bian
  * @Description 会话窗口
  * @Date 2020/8/15 23:43 
  **/
object SessionWindow {
  def main(args: Array[String]): Unit = {

    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream1: DataStream[String] = env.socketTextStream("localhost",9999)

    stream1.print()
    val stream2: DataStream[Record] = stream1.map(data => {
      val arr = data.split(",")
      Record(arr(0), arr(1), arr(2).toInt)
    })

    stream2.print("stream2")

    stream2
      .map(record=>{
        (record.classId,record.age)
      }).keyBy(_._1)
      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
      .reduce((r1,r2)=>{(r1._1,r1._2.min(r2._2))})
      .print("sessionWindow")

    env.execute()
  }
}
