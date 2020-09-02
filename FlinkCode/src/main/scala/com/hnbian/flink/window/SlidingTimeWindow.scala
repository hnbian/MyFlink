package com.hnbian.flink.window

import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @Author haonan.bian
  * @Description 滑动时间窗口
  * @Date 2020/8/15 23:42 
  **/
object SlidingTimeWindow {
  def main(args: Array[String]): Unit = {

    import org.apache.flink.streaming.api.scala._
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream1: DataStream[String] = env.socketTextStream("localhost",9999)

    val stream2: DataStream[Record] = stream1.map(data => {
      val arr = data.split(",")
      Record(arr(0), arr(1), arr(2).toInt)
    })

    //  窗口时间 10 秒,每次滑动 5 秒 默认使用的是 processing time
    stream2.map(record=>{
      (record.classId,record.age)
    }).keyBy(_._1)
      .timeWindow(Time.seconds(10),Time.seconds(5))
      .reduce((r1,r2)=>{(r1._1,r1._2.min(r2._2))})
      .print("minAge")

    env.execute()
  }
}
