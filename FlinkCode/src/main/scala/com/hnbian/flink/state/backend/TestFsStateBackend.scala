package com.hnbian.flink.state.backend

import com.hnbian.flink.common.Obj1
import com.hnbian.flink.state.keyed.TestValueState
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2021/1/21 16:04 
  **/
object TestFsStateBackend extends App {
  // 创建执行环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  import org.apache.flink.runtime.state.filesystem.FsStateBackend
  import org.apache.flink.streaming.api.CheckpointingMode
  import org.apache.flink.streaming.api.environment.CheckpointConfig

  env.enableCheckpointing(5000)

  // 使用文件存储状态
  val stateBackend = new FsStateBackend("file:///opt/flink-1.10.2/checkpoint",true)

  env.setStateBackend(stateBackend)
  // 设置检查点模式（精确一次 或 至少一次）
  env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
  // 设置两次检查点尝试之间的最小暂停时间
  env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
  // 设置检查点超时时间
  env.getCheckpointConfig.setCheckpointTimeout(30 * 1000)
  // 设置可能同时进行的最大检查点尝试次数
  env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
  // 使检查点可以在外部保留
  env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

  val stream1: DataStream[String] = env.socketTextStream("localhost",9999)

  private val value: DataStream[Obj1] = stream1
    .map(data => {
      val arr = data.split(",")
      Obj1(arr(0), arr(1), arr(2).toLong)
    })
  private val value1: KeyedStream[Obj1, String] = value.keyBy(_.id)

  value1
    .process(new TestFsStateBackend)
    .print("TestFsStateBackend")
  env.execute()
}

class TestFsStateBackend extends KeyedProcessFunction[String, Obj1, String]{
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
