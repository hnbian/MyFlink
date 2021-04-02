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
  * @Date 2021/4/2 15:50 
  * */
object CepContiguityStrictNeverTest extends App {
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
  private val pattern: Pattern[Record, Record] = Pattern
    .begin[Record]("start").where(_.age == 20)
    // 定义严格近邻模式，age 为 20 的记录后跟随 classId 不为 3 的记录才会匹配
    .notNext("next").where(_.classId =="3")
  /**
1,小红0,20
2,xx,100 > 匹配 1
    CepContiguityStrictNeverTest:12> start:Record(1,小红0,20),
1,小红1,20
4,xx,200 >未匹配
2,xx,300 >未匹配
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
        val result:StringBuffer = new StringBuffer()
        var start: Record = null
        var next: Record = null

        // 当只有 next 匹配到的时候也会触发，这是 start 未匹配到为空，需要进行判断
        if (null != pattern.get("start")){
          start = pattern.get("start").iterator().next()
          result
            .append("start:")
            .append(start.toString)
            .append(",")
        }

        println(pattern.get("next"))
        if (null != pattern.get("next")){
          next = pattern.get("next").iterator().next()
          result
            .append("next:")
            .append(next.toString)
            .append(",")
        }

        result.toString

      }
    }
  )

  result.print("CepContiguityStrictNeverTest")
  env.execute()
}
