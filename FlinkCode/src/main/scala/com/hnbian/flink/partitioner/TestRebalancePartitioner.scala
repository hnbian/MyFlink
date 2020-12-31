package com.hnbian.flink.partitioner

import org.apache.flink.streaming.api.scala._
/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2020/12/31 15:17 
  **/
object TestRebalancePartitioner extends App{

  // 创建执行环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  // 从自定义的集合中读取数据

  val stream = env.fromCollection(List(1,2,3,4,5,6))
  // 直接打印数据

  stream.rebalance.print().setParallelism(2)
  /**
    * 2> 1
    * 2> 3
    * 2> 5
    *
    * 1> 2
    * 1> 4
    * 1> 6
    */

  env.execute()
}
