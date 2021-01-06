package com.hnbian.flink.window.trigger

import com.hnbian.flink.common.{MinDataReduceFunction, Obj1}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2021/1/4 22:33 
  **/
object TestProcessingTimeTrigger extends App {
  val environment:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  val stream1: DataStream[String] = environment.socketTextStream("localhost",9999)

  val stream2: DataStream[Obj1] = stream1.map(data => {
    val arr = data.split(",")
    Obj1(arr(0), arr(1), arr(2).toInt)
  })

  // 设置一个窗口时间是 10 秒的窗口
  stream2.keyBy(0).window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
    .trigger(ProcessingTimeTrigger.create())
    .reduce(new MinDataReduceFunction)
    .print()

  environment.execute()

}
