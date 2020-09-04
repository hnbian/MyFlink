package com.hnbian.flink.state


import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.scala._


/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2020/9/2 12:08 
  **/
object StateBackendTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val checkpointPath: String = "hdfs:///flink/checkpoints"

    // RocksDBStateBackend
    val backend:StateBackend = new RocksDBStateBackend(checkpointPath)
    env.setStateBackend(backend)

    // MemoryStateBackend
    val memory:StateBackend = new MemoryStateBackend(true)
    env.setStateBackend(memory)

    // FsStateBackend
    val  fsState:StateBackend = new FsStateBackend(checkpointPath)
    env.setStateBackend(fsState)

    env.enableCheckpointing(1000)
    // 配置重启策略
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(60, Time.of(10, TimeUnit.SECONDS)))
  }
}
