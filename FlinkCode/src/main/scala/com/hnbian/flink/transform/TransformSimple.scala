package com.hnbian.flink.transform

import com.hnbian.flink.source.SourceTestList.Record
import org.apache.flink.streaming.api.scala._

/**
  * @Author haonan.bian
  * @Description 简单的 转换算子的使用
  * @Date 2020-07-12 22:23 
  **/
object TransformSimple {

  def main(args: Array[String]): Unit = {


    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // map 算子的使用
    env.fromElements(1, 2, 3, "hello", "world")
      .map(v => {
        v + "-map"
      })
      .print("map")


    /**
      * map:12> world-map
      * map:8> 1-map
      * map:11> hello-map
      * map:10> 3-map
      * map:9> 2-map
      */

    // flatmap 算子的使用

    env.fromCollection(List(
      "hello world", "how are you", "how old are you"
    ))
      .flatMap(str => {
        str.split(" ")
      })
      .print("flatmapStream")

    /**
      * flatmapStream:11> how
      * flatmapStream:12> how
      * flatmapStream:10> hello
      * flatmapStream:12> old
      * flatmapStream:11> are
      * flatmapStream:12> are
      * flatmapStream:10> world
      * flatmapStream:12> you
      * flatmapStream:11> you
      */

    // filter 算子的使用

    env.fromCollection(List(
      "hello world", "how are you", "how old are you"
    ))
      .filter(str => {
        str.contains("how")
      })
      .print("filter")

    /**
      * filter:1> how old are you
      * filter:12> how are you
      */

    env.execute()


  }
}
