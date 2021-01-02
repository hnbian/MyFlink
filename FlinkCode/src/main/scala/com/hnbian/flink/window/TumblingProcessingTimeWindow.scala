package com.hnbian.flink.window

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

  val stream2: DataStream[Record] = stream1.map(data => {
    val arr = data.split(",")
    Record(arr(0), arr(1), arr(2).toInt)
  })

  // 设置一个窗口时间是 10 秒的窗口
  stream2.keyBy(0).window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
    .reduce(new minAgeFunction)
    .print()

  environment.execute()
}

// 计算窗口内年纪最小的记录
class minAgeFunction extends ReduceFunction[Record]{
  override def reduce(r1: Record, r2: Record):Record = {
    if(r1.age < r2.age){
      r1
    }else{
      r2
    }
  }
}
