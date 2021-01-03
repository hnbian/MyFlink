package com.hnbian.flink.watermark

import com.hnbian.flink.common.Obj1
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2021/1/2 18:11 
  **/
object TestCustomAssignerWithPunctuatedWatermarks extends App {

  import org.apache.flink.streaming.api.scala._
  // 创建执行环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  // 设置使用事件时间
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  val stream1: DataStream[String] = env.socketTextStream("localhost",9999)

  val stream2: DataStream[Obj1] = stream1.map(data => {
    val arr = data.split(",")
    Obj1(arr(0), arr(1), arr(2).toLong)
  }).assignTimestampsAndWatermarks(new CustomPunctuatedAssigner)
  env.execute()
}
// 按照自己的规则生成 WaterMark
class CustomPunctuatedAssigner extends AssignerWithPunctuatedWatermarks[Obj1] {

  /** 观察到的最大时间戳 */
  val bound: Long = 60 * 1000

  /**根据数据生成 WaterMark*/
  override def checkAndGetNextWatermark(r: Obj1, extractedTS: Long): Watermark = {
    if (r != null) {
      new Watermark(extractedTS - bound)
    } else {
      null
    }
  }

  // 从数据中抽取时间戳的方式
  def extractTimestamp(r: Obj1, previousTS: Long): Long = {
        r.time
  }
}
