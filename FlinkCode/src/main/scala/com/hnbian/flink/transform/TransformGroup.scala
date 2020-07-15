package com.hnbian.flink.transform

import org.apache.flink.streaming.api.scala._

/**
  * @Author haonan.bian
  * @Description 分组转换算子
  * @Date 2020-07-14 22:37 
  **/
object TransformGroup {

  def main(args: Array[String]): Unit = {

    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.fromCollection(List(
      "hello world", "how are you", "how old are you"
    ))
      .flatMap(str => {
        str.split(" ")
      }).map(v=>{(v,1)})
      .keyBy(0)
      .sum(1).print()

    /**
      * 7> (you,1)
      * 9> (how,1)
      * 6> (are,1)
      * 11> (old,1)
      * 4> (hello,1)
      * 9> (how,2)
      * 6> (are,2)
      * 7> (you,2)
      * 7> (world,1)
      */


    // reduce


    env.execute()
  }
}
