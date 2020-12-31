package com.hnbian.flink.partitioner

import org.apache.flink.streaming.api.scala._

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2020/12/31 16:23 
  **/
object TestKeyGroupStreamPartitioner extends App{

  // 创建执行环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  // 从自定义的集合中读取数据
  val stream = env.fromCollection(List(1,2,3,4,5,6))
  val stream2 = stream.map(v=>{(v%2,v)})
  stream2.setParallelism(2).keyBy(_._1).print("key")
  env.execute()
}
