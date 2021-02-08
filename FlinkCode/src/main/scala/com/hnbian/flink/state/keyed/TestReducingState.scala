package com.hnbian.flink.state.keyed

import com.hnbian.flink.common.Obj1
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.{ReducingState, ReducingStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2021/1/21 16:03 
  **/
object TestReducingState extends App {

  // 创建执行环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val stream1: DataStream[String] = env.socketTextStream("localhost",9999)

  private val value: DataStream[Obj1] = stream1
    .map(data => {
      val arr = data.split(",")
      Obj1(arr(0), arr(1), arr(2).toLong)
    })
  private val value1: KeyedStream[Obj1, String] = value.keyBy(_.id)

  value1
    .process(new TestReducingState)
    .print("TestReducingState")
  env.execute()
}

class TestReducingState extends KeyedProcessFunction[String,Obj1,String]{

  // 定义状态描述符
  private val reducingStateDescriptor = new ReducingStateDescriptor[Obj1]("biggerTime", new Reduce,Types.of[Obj1])
  lazy private val reducingState: ReducingState[Obj1] = getRuntimeContext.getReducingState(reducingStateDescriptor)

  override def processElement(value: Obj1, ctx: KeyedProcessFunction[String, Obj1, String]#Context, out: Collector[String]): Unit = {
    reducingState.add(value)

    out.collect(reducingState.get().toString)
  }
}

// 定义 一个比较 时间的 Reduce Function 函数
class Reduce extends ReduceFunction[Obj1]{
  // 输出时间较大的数据
  override def reduce(value1: Obj1, value2: Obj1): Obj1 = {
    if (value1.time > value2.time){
      value1
    }else{
      value2
    }
  }
}