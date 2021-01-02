package com.hnbian.flink.watermark

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2020/8/19 22:25 
  **/
object WaterMarkTest {

  // 初始化执行环境
  private val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  // 添加 socket source 获取数据
  private val socketStream: DataStream[String] = environment.socketTextStream("localhost", 9999)

}

// 周期性生成 WaterMark
class PeriodicAssiner extends AssignerWithPeriodicWatermarks[String] {

  /** 延迟时间为 1 分钟 */
  val bound:Long = 60 * 1000L

  /** 观察到的最大时间戳 */
  var maxTs: Long = Long.MinValue

  /**生成当前的 WaterMark*/
  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTs - bound)
  }

  /**
    * 抽取时间戳的方法
    * @param timeStamp 数据
    * @param previousElementTimestamp
    * @return
    */
  override def extractTimestamp(timeStamp: String, previousElementTimestamp: Long): Long = {
    maxTs = previousElementTimestamp.max(timeStamp.toLong)
    timeStamp.toLong
  }
}


