package com.hnbian.flink.source

import org.apache.flink.streaming.api.scala._

/**
  * @Author haonan.bian
  * @Description 简单的 source 测试
  * @Date 2020-07-11 10:25 
  **/
object SourceTestList {
  // 定义数据模型样例类
  case class Record(number: Int,value:String)

  def main(args: Array[String]): Unit = {

    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 从自定义的集合中读取数据

    val stream = env.fromCollection(List(
      Record(1,"hello"),
      Record(1,"world")
    ))

    val stream2 = stream.map(record=>{
      Record(record.number*100,record.value+"_map")
    })
    stream2.print("record").setParallelism(1)


    /**
      * record> Record(100,world_map)
      * record> Record(100,hello_map)
      */


    // 从 不同的元素 中读取数据

    val stream3 = env.fromElements(1,2,3,"hello","world")
    stream3.print()

    /**
      * 前面的数字是线程编号
      *
      * 9> world
      * 5> 1
      * 7> 3
      * 6> 2
      * 8> hello
      */

    env.execute()
  }

}
