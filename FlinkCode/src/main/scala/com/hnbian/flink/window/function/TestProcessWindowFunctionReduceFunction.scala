package com.hnbian.flink.window.function

import com.hnbian.flink.common.Obj1
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2021/1/12 17:37 
  **/
object TestProcessWindowFunctionReduceFunction extends App {

  val environment:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  val stream1: DataStream[String] = environment.socketTextStream("localhost",9999)

  val stream2: DataStream[Obj1] = stream1.map(data => {
    val arr = data.split(",")
    Obj1(arr(0), arr(1), arr(2).toInt)
  })

  // 设置一个窗口时间是 10 秒的窗口
  stream2.keyBy(_.id)
    .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
    .reduce(new MinTimeReduceFunction2,new CustomProcessFunction2)
    .print("TestProcessWindowFunctionReduceFunction")

  environment.execute()

}

class CustomProcessFunction2 extends ProcessWindowFunction[Obj1, (Long, Obj1), String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[Obj1], out: Collector[(Long, Obj1)]): Unit = {
    val min = elements.iterator.next
    out.collect((context.window.getStart, min))
  }
}

/**
  * 定义一个 ReduceFunction 比较两个元素的时间大小
  */
class MinTimeReduceFunction2 extends ReduceFunction[Obj1]{
  override def reduce(r1: Obj1, r2: Obj1):Obj1 = {
    if(r1.time > r2.time){
      r1
    }else{
      r2
    }
  }
}



