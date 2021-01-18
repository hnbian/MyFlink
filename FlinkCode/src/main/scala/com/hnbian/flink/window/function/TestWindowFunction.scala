package com.hnbian.flink.window.function

import java.text.SimpleDateFormat
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2021/1/7 16:17 
  **/
object TestWindowFunction extends App {
  val environment:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  val stream1: DataStream[String] = environment.socketTextStream("localhost",9999)

  // 设置一个窗口时间是 5 秒的窗口
  stream1
    .flatMap(_.split(","))
    .map((_,1))
    .keyBy(t =>t._1)
    .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
    .apply(new CustomWindowFunction)
    .print("TestWindowFunction")

  environment.execute()

}

/**
  * (String,Int) –输入值的类型
  * String –输出值的类型
  * String key 类型 –密钥的类型
  * TimeWindow window 类型 –可以应用此窗口功能的 Window 类型
  */
class CustomWindowFunction extends WindowFunction[(String,Int),String,String,TimeWindow]{
  val sdf = new SimpleDateFormat("HH:mm:ss")
  override def apply(key: String,
                     window: TimeWindow,
                     input: Iterable[(String, Int)],
                     out: Collector[String]): Unit = {
    println(
      s"""
         |window key：${key},
         |开始时间：${sdf.format(window.getStart)},
         |结束时间：${sdf.format(window.getEnd)},
         |maxTime：${sdf.format(window.maxTimestamp())}
         |""".stripMargin)

    out.collect(s"${key},${input.map(_._2).sum}")
  }
}