package com.hnbian.flink.watermark

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.windowing.time.Time


/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2021/1/2 00:39 
  **/
object TestBoundedOutOfOrdernessTimestampExtractor extends App{
  import org.apache.flink.streaming.api.scala._
  // 创建执行环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  // 设置使用事件时间
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  val stream1: DataStream[String] = env.socketTextStream("localhost",9999)

  import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
  val stream2: DataStream[Obj3] = stream1.map(data => {
    val arr = data.split(",")
    Obj3(arr(0), arr(1).toLong)
  }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Obj3](Time.seconds(3)) {
    override def extractTimestamp(element: Obj3) = element.time * 1000
  })
  env.execute()
}

case class Obj3(id:String,time:Long)
