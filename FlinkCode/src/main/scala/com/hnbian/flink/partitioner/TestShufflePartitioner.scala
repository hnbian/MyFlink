package com.hnbian.flink.partitioner

import org.apache.flink.streaming.api.scala._

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2020/12/30 22:55 
  **/
object TestShufflePartitioner extends App{
  // 创建执行环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  // 从自定义的集合中读取数据
  val stream = env.fromCollection(List(1,2,3,4,5))
  // 这里只是为了能够将并行度设置为 2
  val stream2 = stream.map(v=>{(v%2,v)}).keyBy(0).map(v=>(v._1,v._2)).setParallelism(2)
  println(stream2.parallelism) // 查看并行度

  stream2.shuffle.print("shuffle").setParallelism(3)
  /**
    * shuffle:3> (0,4)
    * shuffle:2> (1,1)
    * shuffle:1> (0,2)
    * shuffle:3> (1,3)
    * shuffle:1> (1,5)
    */
  env.execute()
}
