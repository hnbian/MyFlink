package com.hnbian.flink.partitioner

import org.apache.flink.streaming.api.scala._

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2020/12/31 15:25 
  **/
object TestRescalePartitioner extends App{

  // 创建执行环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  // 从自定义的集合中读取数据
  val stream = env.fromCollection(List(1,2,3,4,5,6,7,8,9,10,11,12))
  // 这里只是为了能够将并行度设置为 2
  val stream2 = stream.map(v=>{(v%4,v)}).keyBy(0).map(v=>(v._1,v._2)).setParallelism(2)


  //stream2.rescale.print("rescale").setParallelism(2)
  val stream3 = stream2.rescale
  stream3.print().setParallelism(4)
  /**
    * 3> (0,4)
    * 3> (0,8)
    * 3> (1,1)
    * 3> (1,9)
    * 3> (1,5)
    *
    * 4> (2,2)
    * 4> (2,6)
    * 4> (2,10)
    * 4> (3,3)
    * 4> (3,7)
    */

  /**
    * 3> (0,8)
    * 3> (0,4)
    * 3> (1,1)
    * 3> (2,2)
    * 3> (2,6)
    *
    * 4> (1,5)
    * 4> (1,9)
    * 4> (2,10)
    * 4> (3,3)
    * 4> (3,7)
    */
  env.execute()

}
