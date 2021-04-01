package com.hnbian.flink.cep

import java.util
import com.hnbian.flink.common.Record
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2021/4/1 11:27 
  * */
object CepConditionCombiningTest extends App {
  // 创建执行环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  // 1. 获取数据输入流
  val socketStream: DataStream[String] = env.socketTextStream("localhost",9999)

  private val recordStream: DataStream[Record] = socketStream
    .map(data => {
      val arr = data.split(",")
      Record(arr(0), arr(1), arr(2).toInt)
    })

  recordStream.print()
  //#########################################################
  //#########################################################
  //#########################################################

  // 2. 定义一个 Pattern
  private val pattern: Pattern[Record, Record] = Pattern
    // 设置一个简单条件，匹配年龄为 20 的记录
    .begin[Record]("start")
    .where(_.age == 20) // age == 20
    .or( _.age==18 )   // (age==20 || age == 18)
    .where(_.classId=="1") // ((age==20 || age == 18)&& classId == "1")
    .or(_.name == "小明") // ((age==20 || age == 18)&& classId == "1") || name== "小明"

  //#########################################################
  //#########################################################
  //#########################################################

  // 3. 将创建好的 Pattern 应用到输入事件流上
  private val patternStream: PatternStream[Record] = CEP.pattern[Record](recordStream, pattern)

  // 4. 获取事件序列，得到匹配到的数据
  private val result: DataStream[String] = patternStream.select(
    new PatternSelectFunction[Record,String]{
      override def select(pattern: util.Map[String, util.List[Record]]): String = {
        // 从 map 中根据名称获取对应的事件
        val start: Record = pattern.get("start").iterator().next()
        start.toString
      }
    }
  )

  result.print("CepConditionSimpleTest")

  env.execute()
}
