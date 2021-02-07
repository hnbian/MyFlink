package com.hnbian.flink.state.keyed

import com.hnbian.flink.common.Obj1
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2021/1/21 16:02 
  **/
object TestValueState extends App {

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
    .process(new TestValueState)
    .print("TestValueState")
  env.execute()
}

class TestValueState extends KeyedProcessFunction[String, Obj1, String]{
  // 定义状态描述符
  val valueStateDescriptor = new ValueStateDescriptor[Obj1]("objs", Types.of[Obj1])
  lazy val valueState: ValueState[Obj1] = getRuntimeContext.getState(valueStateDescriptor)

  override def processElement(value: Obj1, ctx: KeyedProcessFunction[String, Obj1, String]#Context, out: Collector[String]) = {
    val prev = valueState.value()
    if (null  ==  prev){
      // 更新状态
      valueState.update(value)
    }else{
      // 获取状态
      val obj1 = valueState.value()
      println(s"obj1.time=${obj1.time},value.time=${value.time}")
      if (obj1.time < value.time){
        // 如果 最新数据时间 大于之前时间，更新状态
        valueState.update(value)
      }
    }
    out.collect(value.name)
  }
}
