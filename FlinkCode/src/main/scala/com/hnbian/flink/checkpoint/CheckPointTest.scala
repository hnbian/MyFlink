package com.hnbian.flink.checkpoint

import akka.remote.WireFormats.TimeUnit
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

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

    // 开启 checkpoint 外部持久化，默认 job fail checkpoint 会被清理，RETAIN_ON_CANCELLATION 手动取消任务也需要保留 checkpoint
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    // 设置重启策略
    // 出现故障之后，最多尝试重启 5 次 ,间隔 500 毫秒
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5,500))

    // 失败率重启
    // 失败率
    // 测量失败率的时间间隔
    // 重启时间间隔
    env.setRestartStrategy(RestartStrategies.failureRateRestart(2,Time.seconds(300),Time.seconds(300)))

  }
}
