package com.hnbian.flink.process

import com.hnbian.flink.common.Obj1
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2021/1/21 11:35 
  **/
object TestKeyedProcessFunction extends App {

  // 创建执行环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val stream1: DataStream[String] = env.socketTextStream("localhost",9999)

  private val value: DataStream[Obj1] = stream1
    .map(data => {
      val arr = data.split(",")
      Obj1(arr(0), arr(1), arr(2).toLong)
    })
  private val value1: KeyedStream[Obj1, String] = value.keyBy(_.id)


  value1.process(new CustomKeyedProcessFunction)
    .print("TestKeyedProcessFunction")
  env.execute()
}

/**
  * KeyedProcessFunction
  * String, 输入的 key 的数据类型
  * Obj1, 输入的数据类型
  * String 输出的数据类型
  */
class CustomKeyedProcessFunction extends KeyedProcessFunction[String, Obj1, String]{

  override def processElement(value: Obj1, ctx: KeyedProcessFunction[String, Obj1, String]#Context, out: Collector[String]): Unit = {
    println(s"当前 key:${ctx.getCurrentKey}")
    println(s"当前 ProcessingTime:${ctx.timerService().currentProcessingTime()}")

    out.collect(value.name)
  }
}
