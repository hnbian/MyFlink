package com.hnbian.flink.partitioner

import org.apache.flink.streaming.api.scala._

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2020/12/29 22:55 
  **/
object TestGlobalPartitioner extends App {
  // 创建执行环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  // 从自定义的集合中读取数据

  val stream = env.fromCollection(List(1,2,3,4,5))
  // 直接打印数据
  stream.print()
  /**
    * 打印结果
    * 10> 1
    * 2> 5
    * 1> 4
    * 11> 2
    * 12> 3
    */
  // 使用 GLobal Partitioner 之后打印数据
  stream.global.print("global")

  /**
    * global:1> 1
    * global:1> 2
    * global:1> 3
    * global:1> 4
    * global:1> 5
    */
  env.execute()
}
