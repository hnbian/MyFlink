package com.hnbian.flink.window.function

import com.hnbian.flink.common.Obj1
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2021/1/6 17:07 
  **/
object TestReduceFunction extends App {

  val environment:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  val stream1: DataStream[String] = environment.socketTextStream("localhost",9999)

  val stream2: DataStream[Obj1] = stream1.map(data => {
    val arr = data.split(",")
    Obj1(arr(0), arr(1), arr(2).toInt)
  })

  // 设置一个窗口时间是 10 秒的窗口
  stream2.keyBy(0)
    .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
    .reduce(new MinTimeReduceFunction)
    .print("TestReduceFunction")

  environment.execute()

}

/**
  * 定义一个 ReduceFunction 比较两个元素的时间大小
  */
class MinTimeReduceFunction extends ReduceFunction[Obj1]{
  def reduce(r1: Obj1, r2: Obj1):Obj1 = {

    println(s"r1.time=${r1.time},r2.time=${r2.time}")
    if(r1.time > r2.time){
      println(s"bigger is r1.time=${r1.time}")
      r1
    }else{
      println(s"bigger is r2.time=${r2.time}")
      r2
    }
  }
}