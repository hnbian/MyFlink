package com.hnbian.flink.window

import com.hnbian.flink.common.{MinDataReduceFunction, Obj1}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2021/1/3 15:12 
  **/
object TestSlidingProcessingTimeWindow extends App {

  private val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  private val stream: DataStream[String] = environment.socketTextStream("localhost", 9999)

  stream.map(data=>{
    val dataArray: Array[String] = data.split(",")
    Obj1(dataArray(0),dataArray(1),dataArray(2).toLong)
  }).keyBy(0)
    .window(SlidingProcessingTimeWindows.of(Time.seconds(5),Time.seconds(3)))
    .reduce(new MinDataReduceFunction)
    .print("TestSlidingProcessingTimeWindow")

  environment.execute()
}
