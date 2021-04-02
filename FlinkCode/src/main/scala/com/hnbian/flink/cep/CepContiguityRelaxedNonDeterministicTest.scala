package com.hnbian.flink.cep

import java.util
import com.hnbian.flink.common.Record
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._


/**
  * @Author haonan.bian
  * @Description 非确定性宽松近邻
  * @Date 2021/4/1 11:47 
  * */
object CepContiguityRelaxedNonDeterministicTest  extends App {



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
    .begin[Record]("start")
    .where(_.age == 20)
    // 定义非确定性宽松近邻
    .followedByAny("followedByAny").where(_.classId == "2")
  /**
1,小红0,20 触发 1 触发 2  触发 3 触发 4 触发 5
2,xx,100 触发 1
    CepContiguityRelaxedNonDeterministicTest:2> start:Record(1,小红0,20),followed:Record(2,xx,100),
2,xx,200 触发 2
    CepContiguityRelaxedNonDeterministicTest:3> start:Record(1,小红0,20),followed:Record(2,xx,200),
1,小红1,20  触发 3 触发 4 触发 5
1,小红2,20  触发 3 触发 4 触发 5
2,xx,300 触发 3
    CepContiguityRelaxedNonDeterministicTest:4> start:Record(1,小红0,20),followed:Record(2,xx,300),
    CepContiguityRelaxedNonDeterministicTest:5> start:Record(1,小红1,20),followed:Record(2,xx,300),
    CepContiguityRelaxedNonDeterministicTest:6> start:Record(1,小红2,20),followed:Record(2,xx,300),
1,小红3,20 触发 4 触发 5
1,小红4,20 触发 4 触发 5
1,小红5,20 触发 4 触发 5
3,xx,300
2,xx,300 》 触发 4
    CepContiguityRelaxedNonDeterministicTest:8> start:Record(1,小红1,20),followed:Record(2,xx,300),
    CepContiguityRelaxedNonDeterministicTest:11> start:Record(1,小红4,20),followed:Record(2,xx,300),
    CepContiguityRelaxedNonDeterministicTest:12> start:Record(1,小红5,20),followed:Record(2,xx,300),
    CepContiguityRelaxedNonDeterministicTest:7> start:Record(1,小红0,20),followed:Record(2,xx,300),
    CepContiguityRelaxedNonDeterministicTest:10> start:Record(1,小红3,20),followed:Record(2,xx,300),
    CepContiguityRelaxedNonDeterministicTest:9> start:Record(1,小红2,20),followed:Record(2,xx,300),

2,xx,400 触发 5
  CepContiguityRelaxedNonDeterministicTest:4> start:Record(1,小红3,20),followed:Record(2,xx,400),
  CepContiguityRelaxedNonDeterministicTest:2> start:Record(1,小红1,20),followed:Record(2,xx,400),
  CepContiguityRelaxedNonDeterministicTest:3> start:Record(1,小红2,20),followed:Record(2,xx,400),
  CepContiguityRelaxedNonDeterministicTest:5> start:Record(1,小红4,20),followed:Record(2,xx,400),
  CepContiguityRelaxedNonDeterministicTest:1> start:Record(1,小红0,20),followed:Record(2,xx,400),
  CepContiguityRelaxedNonDeterministicTest:6> start:Record(1,小红5,20),followed:Record(2,xx,400),
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
        var followed: Record = null

        // 当只有 next 匹配到的时候也会触发，这是 start 未匹配到为空，需要进行判断
        if (null != pattern.get("start")){
          start = pattern.get("start").iterator().next()
          result
            .append("start:")
            .append(start.toString)
            .append(",")
        }

        if (null != pattern.get("followedByAny")){
          followed = pattern.get("followedByAny").iterator().next()
          result
            .append("followed:")
            .append(followed.toString)
            .append(",")
        }

        result.toString
      }
    }
  )

  result.print("CepContiguityRelaxedNonDeterministicTest")
  env.execute()
}
