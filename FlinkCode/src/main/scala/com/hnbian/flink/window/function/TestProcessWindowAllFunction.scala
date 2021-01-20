package com.hnbian.flink.window.function

import com.hnbian.flink.common.Obj1
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import scala.collection.mutable.ArrayBuffer

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2021/1/19 23:04 
  **/
object TestProcessWindowAllFunction extends App {
  val environment:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  val stream1: DataStream[String] = environment.socketTextStream("localhost",9999)

  val stream2: DataStream[Obj1] = stream1.map(data => {
    val arr = data.split(",")
    Obj1(arr(0), arr(1), arr(2).toInt)
  })

  // 设置一个窗口时间是 5 秒的窗口
  private val allValueStream: AllWindowedStream[Obj1, TimeWindow] = stream2
    .keyBy(_.id)
    .timeWindowAll(Time.seconds(5))

  allValueStream
    .process(new CustomProcessAllWindowFunction)
    .print("TestProcessWindowAllFunction")

  environment.execute()
}


/**
  * 定义一个ProcessAllWindowFunction
  * 把窗口内所有用户名 拼接成字符串 用 "," 分隔
  * 输入类型 obj1
  * 输出类型  元组(Long,String)
  * 窗口类型 TimeWindow
  */
class CustomProcessAllWindowFunction extends ProcessAllWindowFunction[Obj1, (Long,String), TimeWindow]{
  override def process(context: Context, elements: Iterable[Obj1], out: Collector[(Long, String)]): Unit = {

    println(s"start:${context.window.getStart}")
    println(s"end:${context.window.getEnd}")
    println(s"maxTimestamp:${context.window.maxTimestamp()}")
    val key =  context.window.getStart
    val value = new ArrayBuffer[String]()

    for (obj1<- elements){
      value.append(obj1.name)
    }
    // 把窗口内所有用户名 拼接成字符串 用 "," 分隔
    out.collect((key,value.mkString(",")))
  }
}