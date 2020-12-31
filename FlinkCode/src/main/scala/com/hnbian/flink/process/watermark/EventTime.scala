package com.hnbian.flink.process.watermark

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @Author haonan.bian
  * @Description event time 测试代码
  * @Date 2020/8/18 11:00 
  **/
case class TimeStamp(id:String, timeStamp:Long)
object EventTime {
  def main(args: Array[String]): Unit = {
    import org.apache.flink.streaming.api.scala._
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置使用事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //env.getConfig.setAutoWatermarkInterval(20) // 设置自动 WaterMark
    env.setParallelism(1)

    val stream1: DataStream[String] = env.socketTextStream("localhost",9999)

    val stream2: DataStream[TimeStamp] = stream1.map(data => {
      val arr = data.split(",")
      TimeStamp(arr(0), arr(1).toLong)
    })
      //.assignAscendingTimestamps(_.timeStamp*1000) //  顺序数据，无需指定 WaterMark
      // 设置 延迟 1 秒钟
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[TimeStamp](Time.seconds(1)) {
        // 将当前数据的时间戳设置为 WaterMark
        override def extractTimestamp(element: TimeStamp) = element.timeStamp * 1000
      })


    // 取出 10 秒钟,最小的时间戳
    stream2.keyBy(_.id)
      .timeWindow(Time.seconds(5)) //  窗口数量 默认使用的是 processing time
      .reduce((t1,t2)=>{TimeStamp(t1.id,t1.timeStamp.min(t2.timeStamp))})
      .print("minTimeStamp")

    /**
      * minTimeStamp> TImeStamp(1,1598018500)
      * minTimeStamp> TImeStamp(1,1598018505)
      * minTimeStamp> TImeStamp(1,1598018510)
      * minTimeStamp> TImeStamp(1,1598018515)
      */
    env.execute()
  }


  /**
1,1598018500
1,1598018501
1,1598018502
1,1598018503
1,1598018504
1,1598018505
1,1598018506
1,1598018508
1,1598018509
1,1598018510
1,1598018511
1,1598018512
1,1598018513
1,1598018514
1,1598018515
1,1598018516
1,1598018517
1,1598018518
1,1598018519
1,1598018520
1,1598018521

    */
}
