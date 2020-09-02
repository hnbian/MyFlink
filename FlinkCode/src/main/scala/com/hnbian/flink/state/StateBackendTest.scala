package com.hnbian.flink.state


import java.util.concurrent.TimeUnit
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.scala._


/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2020/9/2 12:08 
  **/
object StateBackendTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment val checkpointPath: String = ???
    val backend = new RocksDBStateBackend(checkpointPath)

    env.setStateBackend(backend)
    env.setStateBackend(new FsStateBackend("file:///tmp/checkpoints"))

    env.enableCheckpointing(1000)
    // 配置重启策略
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(60, Time.of(10, TimeUnit.SECONDS)))
  }
}
