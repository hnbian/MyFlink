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
  * @Date 2021/4/1 11:44 
  * */
object CepQuantifierTimesOrMorTest  extends App {
  // 创建执行环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  // 1. 获取数据输入流
  val socketStream: DataStream[String] = env.socketTextStream("localhost", 9999)

  private val recordStream: DataStream[Record] = socketStream
    .map(data => {
      val arr = data.split(",")
      Record(arr(0), arr(1), arr(2).toInt)
    })


  //#########################################################
  //#########################################################
  //#########################################################


  // 2. 定义一个 Pattern
  // 当 遇见 classId 为 2 之前 累积 age==20 匹配两次或多次时触发
  private val pattern: Pattern[Record, Record] = Pattern
    .begin[Record]("start")
    .where(_.age == 20).timesOrMore(2)
    .next("next").where(_.classId == "2")

/**
1,小红0,20 触发1  触发2
2,xx,100
1,小红1,20 触发1  触发2
1,小红2,20  触发2
2,xx,200 》 触发1
    CepQuantifierTimesOrMorTest:6> Record(1,小红0,20),Record(2,xx,200)
    CepQuantifierTimesOrMorTest:7> Record(1,小红1,20),Record(2,xx,200)
1,小红3,20  触发2
1,小红4,20  触发2
1,小红5,20
2,xx,300 》 触发2
  CepQuantifierTimesOrMorTest:9> Record(1,小红1,20),Record(2,xx,300)
  CepQuantifierTimesOrMorTest:12> Record(1,小红4,20),Record(2,xx,300)
  CepQuantifierTimesOrMorTest:10> Record(1,小红2,20),Record(2,xx,300)
  CepQuantifierTimesOrMorTest:11> Record(1,小红3,20),Record(2,xx,300)
  CepQuantifierTimesOrMorTest:8> Record(1,小红0,20),Record(2,xx,300)
*/

  //#########################################################
  //#########################################################
  //#########################################################

  // 3. 将创建好的 Pattern 应用到输入事件流上
  private val patternStream: PatternStream[Record] = CEP.pattern[Record](recordStream, pattern)

  // 4. 获取事件序列，得到匹配到的数据

  private val result: DataStream[String] = patternStream.select(
    new PatternSelectFunction[Record, String] {
      override def select(pattern: util.Map[String, util.List[Record]]): String = {
        // 从 map 中根据名称获取对应的事件
        val start: Record = pattern.get("start").iterator().next()
        val next: Record = pattern.get("next").iterator().next()
        s"${start.toString},${next.toString}"
      }
    }
  )

  result.print("CepQuantifierTimesOrMorTest")

  env.execute()
}
