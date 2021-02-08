package com.hnbian.flink.state.keyed

import com.hnbian.flink.window.TumblingTimeWIndow.Record
import com.hnbian.flink.window.function.AverageAccumulator
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{AggregatingState, AggregatingStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2021/1/21 16:03 
  **/
object TestAggregatingState extends App {
  // 创建执行环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val stream1: DataStream[String] = env.socketTextStream("localhost",9999)

  private val value: DataStream[Record] = stream1
    .map(data => {
      val arr = data.split(",")
      Record(arr(0), arr(1), arr(2).toInt)
    })
  private val value1: KeyedStream[Record, String] = value.keyBy(_.classId)

  value1
    .process(new TestAggregatingState)
    .print("TestListState")
  env.execute()
}

class TestAggregatingState extends KeyedProcessFunction[String,Record,(ArrayBuffer[Record], Double)]{

  // 定义状态描述符
  private val aggregatingStateDescriptor = new AggregatingStateDescriptor[Record, AverageAccumulator, (ArrayBuffer[Record], Double)]("agg", new AgeAverageAggregateFunction ,Types.of[AverageAccumulator])
  lazy private val aggregatingState: AggregatingState[Record, (ArrayBuffer[Record], Double)] = getRuntimeContext.getAggregatingState(aggregatingStateDescriptor)

  override def processElement(value: Record, ctx: KeyedProcessFunction[String, Record, (ArrayBuffer[Record], Double)]#Context, out: Collector[(ArrayBuffer[Record], Double)]): Unit = {
    aggregatingState.add(value)
    out.collect(aggregatingState.get())
  }
}

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
