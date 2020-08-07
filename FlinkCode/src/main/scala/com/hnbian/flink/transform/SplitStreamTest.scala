package com.hnbian.flink.transform


import org.apache.flink.streaming.api.scala._


/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2020/8/6 16:43 
  **/
object SplitStreamTest {

  def main(args: Array[String]): Unit = {

    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    sideOutput(env)
    env.execute()
    System.exit(-1)
    val stream1: DataStream[Integer] = env.fromElements(1, 2, 3, 4)

    // 使用 split 算子将 DataStream 拆分成多个 DataStream
    val stream2: SplitStream[Integer] = stream1.split(v => {
      if (v % 2 == 0) {
        Seq("even")
      } else {
        Seq("odd")
      }
    })
    stream2.print("SplitStream")
    //    SplitStream:9> 1
    //    SplitStream:10> 2
    //    SplitStream:11> 3
    //    SplitStream:12> 4

    // 使用 select 算子 在SplitStream 中获取一个 DataStream
    val Stream3: DataStream[Integer] = stream2.select("odd")
    Stream3.print("odd")

    //    odd:5> 3
    //    odd:4> 1

    stream2.select("even").print("even")

    //    even:6> 4
    //    even:5> 2
    env.execute()
  }


  /**
    * 使用 side output 分流
    * @param env
    */
  def sideOutput(env: StreamExecutionEnvironment): Unit = {

    val input: DataStream[Int] = env.fromElements(1, 2, 3, 4)
    // 定义 output tag 用于标识输出流
    val evenTag = OutputTag[Int]("even")
    val oddTag = OutputTag[String]("odd")

    import org.apache.flink.util.Collector
    import org.apache.flink.streaming.api.functions.ProcessFunction

    val mainDataStream =
      input.process(new ProcessFunction[Int, Int] {
        override def processElement(
                                     value: Int,
                                     ctx: ProcessFunction[Int, Int]#Context,
                                     out: Collector[Int]): Unit = {
          // 分流逻辑，分为奇数、偶数两个流
          if (value % 2 == 0) {
            ctx.output(evenTag, value)
          } else {
            ctx.output(oddTag, String.valueOf(s"odd-$value"))
          }
        }
      })

    val evenOutputStream: DataStream[Int] = mainDataStream.getSideOutput(evenTag)
    evenOutputStream.print("even")
    //    even:11> 2
    //    even:1> 4

    val oddOutputStream: DataStream[String] = mainDataStream.getSideOutput(oddTag)
    oddOutputStream.print("odd")
//    odd:7> odd-1
//    odd:9> odd-3
  }
}
