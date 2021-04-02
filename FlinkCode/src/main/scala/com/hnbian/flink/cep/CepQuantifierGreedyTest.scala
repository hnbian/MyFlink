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
object CepQuantifierGreedyTest  extends App {


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
    // 匹配在 classId 为 2 之前，出现 2-7 次 age 为 20 的记录，尽可能多的匹配，会出现一条记录多次批评的记录
    .where(_.age == 20).times(2,7).greedy
    .next("next").where(_.classId == "2")
  /**
  1,小红0,20 触发 1 触发 2
2,xx,100
1,小红1,20 触发 1 触发 2
1,小红2,20 触发 2
2,xx,200 触发 1>
          start:Record(1,小红0,20),end:Record(2,xx,200)
          start:Record(1,小红1,20),end:Record(2,xx,200)

1,小红3,20 触发 2 触发 3
1,小红4,20 触发 2 触发 3
1,小红5,20 触发 3 触发 4
2,xx,300 触发 2>
          start:Record(1,小红0,20),end:Record(2,xx,300)
          start:Record(1,小红2,20),end:Record(2,xx,300)
          start:Record(1,小红1,20),end:Record(2,xx,300)
          start:Record(1,小红4,20),end:Record(2,xx,300)
          start:Record(1,小红3,20),end:Record(2,xx,300)

1,小红6,20 触发 3 触发 4 触发 5
1,小红7,20 触发 3 触发 4 触发 5
1,小红8,20 触发 3 触发 4 触发 5
1,小红9,20 触发 4 触发 5
2,xx,400 触发 3 >
          start:Record(1,小红8,20),end:Record(2,xx,400)
          start:Record(1,小红6,20),end:Record(2,xx,400)
          start:Record(1,小红5,20),end:Record(2,xx,400)
          start:Record(1,小红3,20),end:Record(2,xx,400)
          start:Record(1,小红4,20),end:Record(2,xx,400)
          start:Record(1,小红7,20),end:Record(2,xx,400)

1,小红10,20 触发 4 触发 5
1,小红11,20 触发 5
2,xx,400 触发 4 >
            start:Record(1,小红7,20),end:Record(2,xx,400)
            start:Record(1,小红10,20),end:Record(2,xx,400)
            start:Record(1,小红8,20),end:Record(2,xx,400)
            start:Record(1,小红5,20),end:Record(2,xx,400)
            start:Record(1,小红6,20),end:Record(2,xx,400)
            start:Record(1,小红9,20),end:Record(2,xx,400)

1,小红12,20
2,xx,500 触发 5>
          start:Record(1,小红10,20),end:Record(2,xx,500)
          start:Record(1,小红9,20),end:Record(2,xx,500)
          start:Record(1,小红8,20),end:Record(2,xx,500)
          start:Record(1,小红7,20),end:Record(2,xx,500)
          start:Record(1,小红11,20),end:Record(2,xx,500)
          start:Record(1,小红6,20),end:Record(2,xx,500)

2,xx,600 未触发
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
        //
        val result:StringBuffer = new StringBuffer()
        var start: Record = null

        // 当只有 next 匹配到的时候也会触发，这是 start 未匹配到为空，需要进行判断
        if (null != pattern.get("start")){
          start = pattern.get("start").iterator().next()
          result
            .append("start:")
            .append(start.toString)
            .append(",")
        }
        val next: Record = pattern.get("next").iterator().next()
        result.append("end:").append(next.toString)
        result.toString
      }
    }
  )

  result.print("CepQuantifierGreedyTest")

  env.execute()
}
