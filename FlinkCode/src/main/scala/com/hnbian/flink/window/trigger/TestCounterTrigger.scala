package com.hnbian.flink.window.trigger

import com.hnbian.flink.common.Record
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2021/1/5 09:34 
  **/
object TestCounterTrigger extends App {
  // 创建执行环境
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  val stream1: DataStream[String] = env.socketTextStream("localhost",9999)

  val stream2: DataStream[Record] = stream1.map(data => {
    val arr = data.split(",")
    Record(arr(0), arr(1), arr(2).toInt)
  })

  stream2.map(record=>{
    (record.classId,record.age)
  }).keyBy(_._1)
    .timeWindow(Time.seconds(20)) // 默认使用的是 processing time
    .trigger(CountTrigger.of(2)) // 设置 CountTrigger
    .reduce((r1,r2)=>{(r1._1,r1._2.min(r2._2))})
    .print("TestCounterTrigger")
  env.execute()

}
