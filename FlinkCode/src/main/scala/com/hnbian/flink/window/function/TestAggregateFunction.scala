package com.hnbian.flink.window.function

import com.hnbian.flink.window.TumblingTimeWIndow.Record
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import scala.collection.mutable.ArrayBuffer

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2021/1/7 10:45
  **/
object TestAggregateFunction extends App {
  val environment:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  val stream1: DataStream[String] = environment.socketTextStream("localhost",9999)

  val stream2: DataStream[Record] = stream1.map(data => {
    val arr = data.split(",")
    Record(arr(0), arr(1), arr(2).toInt)
  })

  // 设置一个窗口时间是 10 秒的窗口
  stream2.keyBy(0)
    .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
    .aggregate(new AgeAverageAggregateFunction)
    .print("TestReduceFunction")

  environment.execute()
}

// 累加器,保存计算过程中的聚合状态
class AverageAccumulator {
  var records = ArrayBuffer[Record]()
  var count:Long = 0L
  var sum:Long = 0L
}

/**
  * 使用 AggregateFunction 计算最近十秒内流入数据的用户的平均年龄
  */
class AgeAverageAggregateFunction extends AggregateFunction[Record, AverageAccumulator, (ArrayBuffer[Record], Double)] {

  override def getResult(accumulator: AverageAccumulator): (ArrayBuffer[Record], Double)= {//
    val avg = accumulator.sum./(accumulator.count.toDouble)

    (accumulator.records,avg)

  }

  override def merge(a: AverageAccumulator, b: AverageAccumulator): AverageAccumulator = {
    a.count += b.count
    a.sum += b.sum
    a.records.appendAll(b.records)
    a
  }

  override def createAccumulator(): AverageAccumulator = {
    new AverageAccumulator()
  }

  override def add(value: Record, accumulator: AverageAccumulator): AverageAccumulator = {
    accumulator.records.append(value)
    accumulator.sum += value.age
    accumulator.count+=1

    accumulator
  }
}

