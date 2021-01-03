package com.hnbian.flink.watermark

import com.hnbian.flink.common.Obj1
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
  val stream2: DataStream[Obj1] = stream1.map(data => {
    val arr = data.split(",")
    Obj1(arr(0), arr(1), arr(2).toLong)
  }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Obj1](Time.seconds(3)) {
    override def extractTimestamp(element: Obj1) = element.time * 1000
  })
  env.execute()
}
