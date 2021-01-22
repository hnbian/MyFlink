package com.hnbian.flink.process

import com.hnbian.flink.common.Obj1
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2021/1/21 11:35 
  **/
object TestProcessFunction extends App {
  // 创建执行环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val stream1: DataStream[String] = env.socketTextStream("localhost",9999)

  stream1
    .map(data => {
      val arr = data.split(",")
      Obj1(arr(0), arr(1), arr(2).toLong)
    })
    .process(new CustomProcessFunction)
    .print("TestProcessFunction")
  env.execute()
}

class CustomProcessFunction extends ProcessFunction[Obj1,String]{

  /**
    * 处理流中的每个数据，返回 ID 大于 10 的数据与处理数据的 processTime
    * @param value
    * @param ctx
    * @param out
    */
  override def processElement(value: Obj1, ctx: ProcessFunction[Obj1, String]#Context, out: Collector[String]): Unit = {
    if(value.id > "10"){
      out.collect(s"${value.name},${ctx.timerService().currentProcessingTime()}")
    }
  }
}

