package com.hnbian.flink.window

import com.hnbian.flink.common.{MinDataReduceFunction, Obj1}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2021/1/6 16:33 
  **/
object TestAllowedLateness extends App {

  val environment:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  val stream1: DataStream[String] = environment.socketTextStream("localhost",9999)

  val stream2: DataStream[Obj1] = stream1.map(data => {
    val arr = data.split(",")
    Obj1(arr(0), arr(1), arr(2).toInt)
  }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Obj1](Time.seconds(3)) {
    override def extractTimestamp(element: Obj1) = element.time * 1000
  })

  private val outputTag = new OutputTag[Obj1]("lastObj1")
  // 设置一个窗口时间是 10 秒的窗口
  private val stream3: DataStream[Obj1] = stream2.keyBy(0)
    .window(TumblingEventTimeWindows.of(Time.seconds(10)))
    .allowedLateness(Time.seconds(5)) // 在设置允许数据延迟 5 秒
    .sideOutputLateData(outputTag) // 将延迟数据输出到侧输出流中
    .reduce(new MinDataReduceFunction)

  private val lastObj1: DataStream[Obj1] = stream3.getSideOutput(outputTag)

  lastObj1.print("window 迟到数据")
  stream3.print("window 计算结果")

  environment.execute()
}
