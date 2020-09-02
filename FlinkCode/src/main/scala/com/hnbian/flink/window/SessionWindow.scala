package com.hnbian.flink.window

/**
  * @Author haonan.bian
  * @Description 会话窗口
  * @Date 2020/8/15 23:43 
  **/
object SessionWindow {
  def main(args: Array[String]): Unit = {

    import org.apache.flink.streaming.api.scala._
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

  }
}
