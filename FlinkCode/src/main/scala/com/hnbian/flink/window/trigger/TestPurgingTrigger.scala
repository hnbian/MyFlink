package com.hnbian.flink.window.trigger

import com.hnbian.flink.common.{Record}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.{TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{ContinuousProcessingTimeTrigger, PurgingTrigger}
import org.apache.flink.streaming.api.windowing.windows.Window

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2021/1/5 09:47 
  **/
object TestPurgingTrigger extends App {
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
    .window(TumblingProcessingTimeWindows.of(Time.minutes(10)))// 默认使用的是 processing time
    .trigger(PurgingTrigger.of(ContinuousProcessingTimeTrigger.of[Window](Time.seconds(10))))
    .reduce((r1,r2)=>{(r1._1,r1._2.min(r2._2))})
    .print("TestPurgingTrigger")
  env.execute()
}
