package com.hnbian.flink.cep

import java.util
import com.hnbian.flink.common.Record
import org.apache.flink.cep.{PatternFlatSelectFunction, PatternFlatTimeoutFunction}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2021/4/7 14:06 
  * */
object CepSelectFunctionFlatOutTimeTest extends App {

  // 创建执行环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)
  // 1. 获取数据输入流
  val socketStream: DataStream[String] = env.socketTextStream("localhost",9999)

  private val recordStream: DataStream[Record] = socketStream
    .map(data => {
      val arr = data.split(",")
      Record(arr(0), arr(1), arr(2).toInt)
    })

  // 2. 定义一个 Pattern
  private val pattern: Pattern[Record, Record] = Pattern
    // 设置一个简单条件，匹配年龄为 20 的记录
    .begin[Record]("start").where(_.age == 20)
    .next("next").where(_.classId == "2")
    .within(Time.seconds(2))

  // 3. 将创建好的 Pattern 应用到输入事件流上
  private val patternStream: PatternStream[Record] = CEP.pattern[Record](recordStream, pattern)

  // 创建OutputTag,并命名为timeout-output
  val timeoutTag = OutputTag[String]("timeout-output")

  // 4. 获取事件序列，得到匹配到的数据
  private val result: DataStream[String] = patternStream.flatSelect(timeoutTag,new FlatSelectTimeoutFunction,new FlatSelectFunction)

  result.print("CepSelectFunctionFlatOutTimeTest")
  result.getSideOutput(timeoutTag).print("time out")

  env.execute()

}

// 自定义正常事件序列处理函数
class FlatSelectFunction extends PatternFlatSelectFunction[Record,String]{

  override def flatSelect(pattern: util.Map[String, util.List[Record]], out: Collector[String]): Unit = {

    out.collect(
      s"""
         |flatSelectFunction:
         |  ${pattern.get("start").toString},
         |  ${pattern.get("next").toString}
         |""".stripMargin)
  }
}

// 自定义超时事件序列处理函数
class FlatSelectTimeoutFunction extends PatternFlatTimeoutFunction[Record,String]{
  override def timeout(pattern: util.Map[String, util.List[Record]], timeoutTimestamp: Long, out: Collector[String]): Unit = {
    out.collect(
      s"""
         |flatSelectFunction:
         |  ${pattern.get("start").toString}
         |""".stripMargin)
  }
}