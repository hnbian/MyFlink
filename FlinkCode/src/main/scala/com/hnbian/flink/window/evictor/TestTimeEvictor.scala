package com.hnbian.flink.window.evictor

import com.hnbian.flink.common.{MinDataReduceFunction, Obj1}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2021/1/6 16:10 
  **/
object TestTimeEvictor extends App {
  val environment:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  val stream1: DataStream[String] = environment.socketTextStream("localhost",9999)

  val stream2: DataStream[Obj1] = stream1.map(data => {
    val arr = data.split(",")
    Obj1(arr(0), arr(1), arr(2).toInt)
  })

  // 设置一个窗口时间是 10 秒的窗口
  stream2.keyBy(0)
    .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
    // 使用 TimeEvictor 移除器,设置在开窗之前移除数据
    .evictor(TimeEvictor.of(Time.seconds(3),false))
    .reduce(new MinDataReduceFunction)
    .print()
  environment.execute()
}
