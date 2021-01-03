package com.hnbian.flink.window

import com.hnbian.flink.common.{MinDataReduceFunction, Obj1}
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
/**
  * @Author haonan.bian
  * @Description 滚动事件时间窗口
  * @Date 2021/1/1 13:52 
  **/
object TumblingProcessingTimeWindow extends App{

  val environment:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  val stream1: DataStream[String] = environment.socketTextStream("localhost",9999)

  val stream2: DataStream[Obj1] = stream1.map(data => {
    val arr = data.split(",")
    Obj1(arr(0), arr(1), arr(2).toInt)
  })

  // 设置一个窗口时间是 10 秒的窗口
  stream2.keyBy(0).window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
    .reduce(new MinDataReduceFunction)
    .print()

  /**
    * 1,xx,14
    * 2,ww,15
    * 3,dd,12
    */
  environment.execute()
}