package com.hnbian.flink.partitioner

import org.apache.flink.streaming.api.scala._

object TestBroadcastPartitioner extends App {

  // 创建执行环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  // 从自定义的集合中读取数据

  val stream = env.fromCollection(List(1,2,3,4,5))
  // 直接打印数据


  val bro  = stream.broadcast

  bro.print("broadcast").setParallelism(2)
  /**
    * broadcast:1> 1
    * broadcast:1> 2
    * broadcast:1> 3
    * broadcast:1> 4
    * broadcast:1> 5
    *
    * broadcast:2> 1
    * broadcast:2> 2
    * broadcast:2> 3
    * broadcast:2> 4
    * broadcast:2> 5
    */

  env.execute()
}
