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
  * @Date 2021/4/2 16:49 
  * */
object CepContiguityNotFollowedByTest extends App{

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
    .notFollowedBy("notFollowedBy").where(_.age == 11)
    .next("next").where(_.age == 15)
    .next("next2").where(_.age == 18)

  /**
1,小红0,20
2,xx,15
1,小红1,18 触发

1,小红0,20
2,xx,15
3,xx,11
1,小红1,18

1,小红0,20
4,xx,11
2,xx,15
1,小红1,18


1,小红0,20
4,xx,12
2,xx,15
1,小红1,18

1,小红0,20
4,xx,22
2,xx,15
1,小红1,18

1,小红0,20
2,xx,15
1,小红1,18


2,xx,22
2,xx,15
1,小红2,20
2,xx,400
1,小红3,15
4,小红3,20  触发 2
2,xx,15 》 触发 2
    CepContiguityNotFollowedByTest:8> start:Record(4,小红3,20),next:Record(2,xx,15),
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
        var notFollowedBy: Record = null
        var next: Record = null
        var next2: Record = null

        // 当只有 next 匹配到的时候也会触发，这是 start 未匹配到为空，需要进行判断
        if (null != pattern.get("start")){
          start = pattern.get("start").iterator().next()
          result
            .append("start:")
            .append(start.toString)
            .append(",")
        }

        println("notFollowedBy=="+pattern.get("notFollowedBy"))
        if (null != pattern.get("notFollowedBy")){
          notFollowedBy = pattern.get("notFollowedBy").iterator().next()
          result
            .append("notFollowedBy:")
            .append(notFollowedBy.toString)
            .append(",")
        }

        if (null != pattern.get("next")){
          next = pattern.get("next").iterator().next()
          result
            .append("next:")
            .append(next.toString)
            .append(",")
        }

        if (null != pattern.get("next2")){
          next2 = pattern.get("next2").iterator().next()
          result
            .append("next2:")
            .append(next2.toString)
            .append(",")
        }

        result.toString

      }
    }
  )

  result.print("CepContiguityNotFollowedByTest")
  env.execute()

}
