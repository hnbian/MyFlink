package com.hnbian.flink.transform

import org.apache.flink.streaming.api.scala.DataStream

import org.apache.flink.streaming.api.scala._

object KeyedStreamTest{

  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    dataStreamToKeyedStream(env)
    env.execute()
  }

  /**
    * dataStream 转换为 KeyedStream
    * @param env
    */
  def dataStreamToKeyedStream(env:StreamExecutionEnvironment): Unit ={

    val stream1:DataStream[String] = env.fromElements ("hello world","how are you","hello flink","flink datastream")


    import org.apache.flink.streaming.api.scala.KeyedStream
    import org.apache.flink.api.java.tuple.Tuple
    val stream2:KeyedStream[(String, Int), Tuple] = stream1
      .flatMap(v=>{v.split(" ")})
      .map((_,1))
      .keyBy(0)

    stream2.print("KeyedStream")
//    KeyedStream> (hello,1)
//    KeyedStream> (world,1)
//    KeyedStream> (how,1)
//    KeyedStream> (are,1)
//    KeyedStream> (you,1)
//    KeyedStream> (hello,1)
//    KeyedStream> (flink,1)
//    KeyedStream> (flink,1)
//    KeyedStream> (datastream,1)
  }
}
