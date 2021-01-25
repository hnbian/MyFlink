package com.hnbian.flink.process

import com.hnbian.flink.common.Obj1
import com.hnbian.flink.common.Record
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2021/1/21 11:36 
  **/
object TestCoProcessFunction extends App {
  // 创建执行环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val stream1: DataStream[String] = env.socketTextStream("localhost",9999)
  val stream2: DataStream[String] = env.socketTextStream("localhost",8888)

  private val stream1Obj: DataStream[Obj1] = stream1
    .map(data => {
      val arr = data.split(",")
      Obj1(arr(0), arr(1), arr(2).toLong)
    })

  val stream2Rec: DataStream[Record] = stream2.map(data => {
    val arr = data.split(",")
    Record(arr(0), arr(1), arr(2).toInt)
  })

  stream1Obj
    .connect(stream2Rec)
    .process(new CustomCoProcessFunction)
    .print()

  env.execute()
}

class CustomCoProcessFunction extends CoProcessFunction[Obj1,Record,String]{
  override def processElement1(value: Obj1, ctx: CoProcessFunction[Obj1, Record, String]#Context, out: Collector[String]): Unit = {
    out.collect(s"processElement1:${value.name},${value.getClass}")
  }

  override def processElement2(value: Record, ctx: CoProcessFunction[Obj1, Record, String]#Context, out: Collector[String]): Unit = {
    out.collect(s"processElement2:${value.name},${value.getClass}")
  }
}