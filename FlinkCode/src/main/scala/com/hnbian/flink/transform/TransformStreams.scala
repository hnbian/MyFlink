package com.hnbian.flink.transform

import org.apache.flink.streaming.api.scala._

/**
  * @Author haonan.bian
  * @Description 多流转换算子
  * @Date 2020-07-20 22:30 
  **/
object TransformStreams {
  def main(args: Array[String]): Unit = {

    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.fromCollection(List(1,2,3,4,5,6,7,8,9))
      .split(data=>{
      if(data%2 == 0){
        Seq("a")
      }else{
        Seq("b")
      }
    })

    val a = stream.select("a")

    val b = stream.select("b")

    val all = stream.select("a","b")

    //a.print("a")

    /**
      * a:10> 2
      * a:1> 8
      * a:12> 6
      * a:11> 4
      */


    //b.print("b")

    /**
      * b:11> 9
      * b:7> 1
      * b:8> 3
      * b:9> 5
      * b:10> 7
      */

    all.print("all")

    /**
      * all:4> 1
      * all:5> 2
      * all:6> 3
      * all:12> 9
      * all:10> 7
      * all:9> 6
      * all:8> 5
      * all:7> 4
      * all:11> 8
      */


    env.execute()
    // split
    // select
  }

}
