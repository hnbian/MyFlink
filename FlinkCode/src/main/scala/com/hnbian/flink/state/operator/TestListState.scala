package com.hnbian.flink.state.operator

import com.hnbian.flink.transform.DataStreamTest.setMaxParallelism
import org.apache.flink.api.common.state.ListState
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2021/1/21 16:01 
  **/
object TestListState extends App {

  // 创建执行环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  var list : ListState[(String,Int)] = null


  setMaxParallelism(env)
  env.execute()


  //private var checkpointedState: ListState[(String, Int)] = _
}
