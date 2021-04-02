package com.hnbian.flink.cep

import java.util
import com.hnbian.flink.common.Record
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._


/**
  * @Author haonan.bian
  * @Description 严格近邻
  * @Date 2021/4/1 11:47 
  * */
object CepContiguityStrictTest  extends App {



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
    // 定义严格近邻模式，age 为 20 的记录后必须紧跟 classId 为 2 的记录才会匹配
    .next("next").where(_.classId == "2")
/**
1,小红0,20 》 匹配 1
2,xx,100 》 匹配 1
            CepContiguityStrictTest:6> Record(1,小红0,20),Record(2,xx,100)
1,小红1,20 》未匹配
1,小红2,20 》未匹配
3,xx,200 》未匹配
1,小红3,20 》未匹配
1,小红4,20 》未匹配
1,小红5,20 》 匹配 2
2,xx,300 》 匹配 2
        CepContiguityStrictTest:7> Record(1,小红5,20),Record(2,xx,300)
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

  result.print("CepContiguityStrictTest")

  env.execute()

}
