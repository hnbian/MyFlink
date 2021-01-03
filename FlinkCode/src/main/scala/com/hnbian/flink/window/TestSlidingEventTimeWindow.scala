package com.hnbian.flink.window

import com.hnbian.flink.common.{MinDataReduceFunction, Obj1}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.{AscendingTimestampExtractor}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2021/1/3 15:26 
  **/
object TestSlidingEventTimeWindow extends App {

  val environment:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  val stream1: DataStream[String] = environment.socketTextStream("localhost",9999)
  stream1.print("stream1")
  val stream2: DataStream[Obj1] = stream1.map(data => {
    val arr = data.split(",")
    //println(arr)
    Obj1(arr(0), arr(1), arr(2).toLong)
  }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor[Obj1] {
    override def extractAscendingTimestamp(element: Obj1) = {
      // 提取当前的 EventTime，会设置当前的 EventTime 为 WaterMark
      element.time * 1000
    }
  })

  stream2.keyBy(0)
    .window(SlidingEventTimeWindows.of(Time.seconds(6),Time.seconds(3)))
    .reduce(new MinDataReduceFunction)
    .print("TestSlidingEventTimeWindow")

  environment.execute()
}
