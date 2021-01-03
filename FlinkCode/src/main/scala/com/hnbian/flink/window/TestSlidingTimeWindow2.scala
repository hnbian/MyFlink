package com.hnbian.flink.window
import com.hnbian.flink.common.{MinDataReduceFunction, Obj1}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api._
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.SlidingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2021/1/3 18:36 
  **/
object TestSlidingTimeWindow2 {
  val environment:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  val stream1: DataStream[String] = environment.socketTextStream("localhost",9999)
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
    .window(SlidingTimeWindows.of(Time.seconds(6),Time.seconds(3)))
    .reduce(new MinDataReduceFunction)
    .print("TestSlidingEventTimeWindow")

  environment.execute()
}
