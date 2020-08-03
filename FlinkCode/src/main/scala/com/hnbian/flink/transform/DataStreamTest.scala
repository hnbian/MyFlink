package com.hnbian.flink.transform

import org.apache.flink.streaming.api.scala._

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2020-07-31 16:33 
  **/
object DataStreamTest {

  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //env.setParallelism(1)

    global(env)
    env.execute()
  }


  /**
    * 集数据流的分割，使输出值都去到下一个加转换算子的的第一个实例。
    * 请谨慎使用此设置，因为它可能会导致应用程序出现严重的性能瓶颈。
    * @param env
    */
  def global(env:StreamExecutionEnvironment): Unit ={
    val stream1:DataStream[Integer] = env.fromElements(1,2,3,4,5,6,7)
    stream1.print("stream1")

    val stream2:DataStream[Integer] = stream1.global

    stream2.print("global")


//    stream1:10> 6
//    stream1:7> 3
//    stream1:8> 4
//    stream1:11> 7
//    stream1:9> 5
//    stream1:6> 2
//    stream1:5> 1

//    global:1> 1
//    global:1> 2
//    global:1> 3
//    global:1> 4
//    global:1> 5
//    global:1> 6
//    global:1> 7

  }

  /**
    * DataStream 合并
    * 注意 DataStream 中的数据类型要一致 才可以做 union 操作
    * @param env
    */
  def union(env:StreamExecutionEnvironment): Unit ={
    val stream1:DataStream[Integer] = env.fromElements(1,2)
    val stream2:DataStream[Integer] = env.fromElements(3,4)
    val stream3:DataStream[Integer] = env.fromElements(5,6)

    val stream4:DataStream[Integer] = stream1.union(stream2,stream3)

    stream4.print("union")
//    union> 3
//    union> 4
//    union> 5
//    union> 6
//    union> 1
//    union> 2
  }

  def filter(env:StreamExecutionEnvironment): Unit ={

    val stream1:DataStream[Integer] = env.fromElements(1,2,3,4,5,6,7)

    val stream2:DataStream[Integer]= stream1.filter(v => {
      v > 5
    })

    stream2.print("filter")
//    filter> 6
//    filter> 7

  }

  def flatmap(env:StreamExecutionEnvironment): Unit ={
    val stream1:DataStream[String] = env.fromElements("hello world", "flink demo")

    val stream2:DataStream[String]= stream1.flatMap(v => {
      v.split(" ")
    })

    stream2.print("flatMap")
//    flatMap> hello
//    flatMap> world
//    flatMap> flink
//    flatMap> demo

  }

  /**
    * map 算子的使用
    * @param env
    */
  def map(env:StreamExecutionEnvironment): Unit ={

    val stream1:DataStream[String] = env.fromElements("hello", "world")

    val stream2:DataStream[String]= stream1.map(v => {
      v + "-map"
    })

    stream2.print("map")
//    map> hello-map
//    map> world-map
  }
}
