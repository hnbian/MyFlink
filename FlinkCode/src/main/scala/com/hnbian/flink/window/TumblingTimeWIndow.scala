package com.hnbian.flink.window

import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @Author haonan.bian
  * @Description 滚动时间窗口
  * @Date 2020/8/15 23:41 
  **/


object TumblingTimeWIndow {
  def main(args: Array[String]): Unit = {

    import org.apache.flink.streaming.api.scala._
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream1: DataStream[String] = env.socketTextStream("localhost",9999)

    val stream2: DataStream[Record] = stream1.map(data => {
      val arr = data.split(",")
      Record(arr(0), arr(1), arr(2).toInt)
    })



    // 取出 10 秒钟之内,每个 classId 年纪最小的用户
    stream2.map(record=>{
      (record.classId,record.age)
    }).keyBy(_._1)
      .timeWindow(Time.seconds(10)) //  窗口时间 默认使用的是 processing time
      .reduce((r1,r2)=>{(r1._1,r1._2.min(r2._2))})
      .print("minAge")

    /**
      * minAge:6> (1,12)
      * minAge:3> (2,11)
      */

    /*
1,xiaoming,12
2,xiaodong,11
1,xioahong,13
1,xiaogang,14
1,xiaogang,15
2,xiaohuang,15

1,xiaoming,121
2,xiaodong,111
    */

    env.execute()
  }
}
