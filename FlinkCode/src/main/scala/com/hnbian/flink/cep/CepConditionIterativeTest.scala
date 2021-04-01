package com.hnbian.flink.cep

import java.util

import com.hnbian.flink.common.Record
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.pattern.conditions.IterativeCondition
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._

/**
  * @Author haonan.bian
  * @Description 迭代条件
  * @Date 2021/4/1 11:28 
  * */
object CepConditionIterativeTest  extends App {

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
    // 匹配年龄为 20 或年龄为 18 的记录
    .begin[Record]("start")
    .where(_.age == 20)
    .or( _.age==18 )   // (age==20 || age == 18)
    .where(new MyIterativeCondition())

  // 定义一个迭代条件类
  class MyIterativeCondition extends IterativeCondition[Record]{
    override def filter(value: Record, ctx: IterativeCondition.Context[Record]): Boolean = {
      if(value.name=="小明"){
        false
      }else if (value.classId == "1"){
        true
      }else{
        false
      }
    }
  }

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

  result.print("CepConditionIterativeTest")

  env.execute()
}
