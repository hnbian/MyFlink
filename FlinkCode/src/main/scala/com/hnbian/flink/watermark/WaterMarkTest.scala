package com.hnbian.flink.watermark

import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.watermark.Watermark

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2020/8/19 22:25 
  **/
object WaterMarkTest {

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

// 按照自己的规则生成 WaterMark
class PunctuatedAssigner extends AssignerWithPunctuatedWatermarks[String] {

  /** 观察到的最大时间戳 */
  val bound: Long = 60 * 1000

  /**根据数据生成 WaterMark*/
  override def checkAndGetNextWatermark(r: String, extractedTS: Long): Watermark = {
    if (r != null) {
      new Watermark(extractedTS - bound)
    } else {
      null
    }
  }

  // 从数据中抽取时间戳的方式
  override def extractTimestamp(r: String, previousTS: Long): Long = {
    r.toLong
  }
}
