package com.hnbian.flink.cep

import java.util

import com.hnbian.flink.common.Record
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2020/10/21 10:10 
  **/
object CEPTest extends App{

  // 创建执行环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  //获取数据输入流
  val stream1: DataStream[String] = env.socketTextStream("localhost",9999)
  private val value: DataStream[Record] = stream1
    .map(data => {
      val arr = data.split(",")
      Record(arr(0), arr(1), arr(2).toInt)
    })


  // 定义一个 Pattern， 匹配两秒内处理到的年龄都是 20 岁的两个人
  private val pattern: Pattern[Record, Record] = Pattern
    .begin[Record]("start").where(_.age == 20) // 匹配第一个获取到的年龄是 20 岁
    //.next("next").where(_.age == 20) // 匹配第二个获取到的年龄是 20 岁
    .within(Time.seconds(2)) // 设定约束时间为 2 秒


  // 将创建好的 Pattern 应用到输入事件流上
  private val patternStream: PatternStream[Record] = CEP.pattern[Record](value, pattern)

  // 获取事件序列，得到匹配到的数据
  private val result: DataStream[String] = patternStream.select(new PatternMacthData)
  result.print("CEPTest")

  env.execute()
}

/**
  * 定义一个Pattern Select Function 对匹配到的数据进行处理
  */
class PatternMacthData extends PatternSelectFunction[Record,String]{
  override def select(pattern: util.Map[String, util.List[Record]]): String = {
    // 从 map 中根据名称获取对应的事件
    val start: Record = pattern.get("start").iterator().next()
    val next: Record = pattern.get("next").iterator().next()

    s"${start.name} 与 ${next.name} 都是 ${start.age} 岁"
  }
}
