package com.hnbian.flink.partitioner

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.scala._
/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2020/12/31 16:36 
  **/
object CustomPartitioner extends Partitioner[String]{
  override def partition(key: String, numPartitions: Int): Int = {
    // 根据 key值的 奇偶 返回到不同的分区
    key.toInt % 2
  }
}

object TestCustomPartitioner extends App{
  // 创建执行环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  // 从自定义的集合中读取数据

  val stream = env.fromCollection(List("1","2","3","4","5"))

  val partitioner =  CustomPartitioner

  val stream2 = stream.map(value=>{((value.toInt%2).toString,value)})
  stream2.partitionCustom(partitioner,0).print().setParallelism(2)
  /**
    * 1> (0,2)
    * 1> (0,4)
    * 2> (1,1)
    * 2> (1,5)
    * 2> (1,3)
    */
  env.execute()
}


