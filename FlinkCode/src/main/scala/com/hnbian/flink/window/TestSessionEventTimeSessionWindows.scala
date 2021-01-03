package com.hnbian.flink.window

import com.hnbian.flink.common.{MinDataReduceFunction, Obj1}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, ProcessingTimeSessionWindows}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2021/1/3 19:28 
  **/
object TestSessionEventTimeSessionWindows extends App {
  // 创建执行环境
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  val stream1: DataStream[String] = env.socketTextStream("localhost",9999)

  val stream2: DataStream[Obj1] = stream1.map(data => {
    val arr = data.split(",")
    Obj1(arr(0), arr(1), arr(2).toLong)
  }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor[Obj1] {
    override def extractAscendingTimestamp(element: Obj1) = {
      // 提取当前的 EventTime，会设置当前的 EventTime 为 WaterMark
      element.time * 1000
    }
  })

  stream2.keyBy(0)
    .window(EventTimeSessionWindows.withGap(Time.seconds(5)))
    .reduce(new MinDataReduceFunction)
    .print("EventTimeSessionWindows")

  env.execute()

}
