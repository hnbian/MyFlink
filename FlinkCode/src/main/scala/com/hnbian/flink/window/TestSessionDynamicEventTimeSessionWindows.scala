package com.hnbian.flink.window

import com.hnbian.flink.common.{MinDataReduceFunction, Obj1}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.windowing.triggers.Trigger
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{DynamicEventTimeSessionWindows, SessionWindowTimeGapExtractor}

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2021/1/3 20:10 
  **/
object TestSessionDynamicEventTimeSessionWindows extends App {
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  val stream1 = env.socketTextStream("localhost",9999)

  val stream2: DataStream[Obj1] = stream1.map(data => {
    val arr = data.split(",")
    Obj1(arr(0), arr(1), arr(2).toLong)
  }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor[Obj1] {
     def extractAscendingTimestamp(element: Obj1) ={
      element.time * 1000
    }
  })

  val sessionGap = new SessionWindowTimeGapExtractor[Obj1](){
    // 动态返回时间间隔,单位毫秒
    override def extract(element: Obj1) = {
      // 设置 当 ID 大于等于 2 时 session gap 时间为 3 秒 ，其余为 5 秒
      if (element.id >= "2"){
        3000L
      }else{
        5000L
      }
    }
  }

  stream2.keyBy(0)
    .window(
      DynamicEventTimeSessionWindows.withDynamicGap(sessionGap)
    )
    .reduce(new MinDataReduceFunction)
    .print("EventTimeSessionWindows")

  env.execute()
}
