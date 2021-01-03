package com.hnbian.flink.watermark

import com.hnbian.flink.common.Obj1
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2021/1/2 00:59 
  **/
object TestCustomAssignerWithPeriodicWatermarks {
  import org.apache.flink.streaming.api.scala._
  // 创建执行环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  // 设置使用事件时间
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  val stream1: DataStream[String] = env.socketTextStream("localhost",9999)

  val stream2: DataStream[Obj1] = stream1.map(data => {
    val arr = data.split(",")
    Obj1(arr(0), arr(1), arr(2).toLong)
  }).assignTimestampsAndWatermarks(new CustomPeriodicAssiner)
  env.execute()
}

// 周期性生成 WaterMark
class CustomPeriodicAssiner extends AssignerWithPeriodicWatermarks[Obj1] {

  /** 延迟时间为 1 分钟 */
  val bound: Long = 60 * 1000L

  /** 观察到的最大时间戳 */
  var maxTs: Long = Long.MinValue

  /** 生成当前的 WaterMark */
  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTs - bound)
  }

  /**
    * 抽取时间戳的方法
    * @param timeStamp 数据
    * @param previousElementTimestamp
    * @return
    */
   def extractTimestamp(timeStamp: Obj1, previousElementTimestamp: Long): Long = {
    maxTs = previousElementTimestamp.max(timeStamp.time)
    timeStamp.time
  }
}