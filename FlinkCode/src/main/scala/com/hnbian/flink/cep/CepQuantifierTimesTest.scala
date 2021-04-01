package com.hnbian.flink.cep

import java.util
import com.hnbian.flink.common.Record
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._

/**
  * @Author haonan.bian
  * @Description 量词 times 测试代码
  * @Date 2021/4/1 11:32 
  * */
object CepQuantifierTimesTest  extends App {
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
/*  private val pattern: Pattern[Record, Record] = Pattern
    .begin[Record]("start")
    .where(_.age == 20).times(2)
    .next("next").where(_.classId == "2")*/
/*
  1,小红1,20
  1,小红2,20
  2,小小小小小小,22 》 Record(1,小红1,20),Record(2,小小小小小小,22)
  1,小明3,20
  1,小红4,20
  1,小红5,20
  2,小小小小小小,22 》Record(1,小红4,20),Record(2,小小小小小小,22)
*/



  // 当 遇见 classId 为 2 之前 累积 age 匹配次数为 2或3或4 次时触发
  private val pattern: Pattern[Record, Record] = Pattern
    .begin[Record]("start")
    .where(_.age == 20).times(2, 4)
    .next("next").where(_.classId == "2")

  /**
  1,小红0,20 -》触发 1
2,xx,100 未触发，因为 age==20  只累积一次

1,小红1,20 -》触发 1
1,小红2,20 -》触发2
2,xx,200 触发1 》 Record(1,小红1,20),Record(2,xx,200)
                Record(1,小红0,20),Record(2,xx,200)

1,小红3,20 -》触发2
1,小红4,20 -》触发2
1,小红5,20 -》触发 3 前累积5 次 超过最大值的 4次，故丢弃该数据
2,xx,300 触发2 》 Record(1,小红3,20),Record(2,xx,300)
                Record(1,小红2,20),Record(2,xx,300)
                Record(1,小红4,20),Record(2,xx,300)

1,小红6,20 触发3
1,小红7,20 触发3
1,小红8,20 触发3
1,小红9,20 -》触发 3 前累积7 次 超过最大值的 4次，故丢弃该数据
2,xx,400 触发3 》Record(1,小红6,20),Record(2,xx,400)
                Record(1,小红8,20),Record(2,xx,400)
                Record(1,小红7,20),Record(2,xx,400)

1,小红10,20 -》触发 3 前累积7 次 超过最大值的 4次，故丢弃该数据
1,小红11,20 触发4
1,小红12,20 触发4
1,小红13,20 触发4
1,小红14,20 未触发
2,xx,400 触发4 》Record(1,小红11,20),Record(2,xx,400)
                Record(1,小红12,20),Record(2,xx,400)
                Record(1,小红13,20),Record(2,xx,400)
2,xx,500 未触发
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


  result.print("CepQuantifierTimesTest")

  env.execute()
}
