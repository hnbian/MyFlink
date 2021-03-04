package com.hnbian.flink.state.backend

import com.hnbian.flink.common.Obj1
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2021/1/21 16:05 
  **/
object TestRocksDBStateBackend extends App {
  // 状态后端数据存储应该存储在分布式文件系统里，便于管理维护
  System.setProperty("HADOOP_USER_NAME", "root")
  System.setProperty("hadoop.home.dir", "/usr/hdp/3.1.0.0-78/hadoop//bin/")

  private val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 只有开启了checkpoint，重启策略才会生效；默认不开启重启策略
    // 开启，检查点周期，单位毫秒；默认是-1，不开启
    environment.enableCheckpointing(5000);

    // 默认的重启策略是固定延迟无限重启, 该配置指定在重新启动的情况下将用于执行图的重新启动策略
    // environment.getConfig().setRestartStrategy(RestartStrategies.fallBackRestart());

    // 设置固定延迟固定次数重启；默认是无限重启
    environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000))

    val stateBackend = new RocksDBStateBackend("file:///opt/flink-1.10.2/checkpoint")

    environment.setStateBackend(stateBackend)

    // 程序异常退出或人为cancel掉，不删除checkpoint的数据；默认是会删除Checkpoint数据
    environment.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

  val stream1: DataStream[String] = environment.socketTextStream("localhost",9999)

  private val value: DataStream[Obj1] = stream1
    .map(data => {
      val arr = data.split(",")
      Obj1(arr(0), arr(1), arr(2).toLong)
    })
  private val value1: KeyedStream[Obj1, String] = value.keyBy(_.id)

  value1
    .process(new TestRocksDBStateBackend)
    .print("TestRocksDBStateBackend")
  environment.execute()

}

class TestRocksDBStateBackend extends KeyedProcessFunction[String, Obj1, String]{
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
