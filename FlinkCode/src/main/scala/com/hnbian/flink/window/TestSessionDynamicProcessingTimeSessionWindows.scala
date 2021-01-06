package com.hnbian.flink.window

import com.hnbian.flink.common.{MinDataReduceFunction, Obj1}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{DynamicProcessingTimeSessionWindows, EventTimeSessionWindows, SessionWindowTimeGapExtractor}

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2021/1/3 19:49
  **/
object TestSessionDynamicProcessingTimeSessionWindows extends App {
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  val stream1 = env.socketTextStream("localhost",9999)

  val stream2: DataStream[Obj1] = stream1.map(data => {
    val arr = data.split(",")
    Obj1(arr(0), arr(1), arr(2).toLong)
  })

  val sessionGap = new SessionWindowTimeGapExtractor[Obj1](){
    // 动态返回时间间隔,单位毫秒
    override def extract(element: Obj1) = {
      // 设置 当 ID 大于等于 2 时 session gap 时间为 3 秒 ，其余为 5 秒
      if (element.id >= "2"){
        3000L
      }else{
        5000L
      }
    }
  }

  stream2.keyBy(0)
    .window(
      DynamicProcessingTimeSessionWindows.withDynamicGap(sessionGap)
    )
    .reduce(new MinDataReduceFunction)
    .print("EventTimeSessionWindows")

  env.execute()
}


