package com.hnbian.flink.checkpoint

import com.hnbian.flink.common.Obj1
import com.hnbian.flink.state.backend.TestFsStateBackend
import com.hnbian.flink.state.backend.TestFsStateBackend.{env, stateBackend}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2020/9/2 22:39 
  **/
object CheckPointTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment


    // 启动 checkpoint 并设置时间间隔为 1000 毫秒  = 1 秒
    env.enableCheckpointing(1000)

    // 设置状态一致性级别
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)

    // 设置 checkpoint 超时时间，超时则该次 checkpoint 丢弃
    env.getCheckpointConfig.setCheckpointTimeout(10000)

    // 当检查点出错的时候是否把整个任务 fail 掉，默认 true
    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)

    // 设置同时启动 checkpoint 的最大数量
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)

    // 设置两次 checkpoint 最小时间间隔
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(5000)

    // 开启 checkpoint 外部持久化
    // 默认 job fail checkpoint 会被清理，
    // RETAIN_ON_CANCELLATION 手动取消任务也需要保留 checkpoint
    env
      .getCheckpointConfig
      .enableExternalizedCheckpoints(
        ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
      )

    // 设置重启策略
    // 出现故障之后，最多尝试重启 5 次 ,间隔 500 毫秒
    env.setRestartStrategy(
      RestartStrategies.fixedDelayRestart(5,500)
    )

    // 失败率重启方法
    // 失败率
    // 测量失败率的时间间隔
    // 重启时间间隔
    env.setRestartStrategy(
      RestartStrategies
        .failureRateRestart(2,Time.seconds(300),Time.seconds(300))
    )

    // 使用文件存储状态
    val stateBackend = new FsStateBackend("file:///opt/flink-1.10.2/checkpoint",true)

    env.setStateBackend(stateBackend)

    val stream1: DataStream[String] = env.socketTextStream("localhost",9999)

    val value: DataStream[Obj1] = stream1
      .map(data => {
        val arr = data.split(",")
        Obj1(arr(0), arr(1), arr(2).toLong)
      })
    val value1: KeyedStream[Obj1, String] = value.keyBy(_.id)

    value1
      .process(new CheckPointTest)
      .print("CheckPointTest")
    env.execute()

  }
}
class CheckPointTest extends KeyedProcessFunction[String, Obj1, String]{
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
