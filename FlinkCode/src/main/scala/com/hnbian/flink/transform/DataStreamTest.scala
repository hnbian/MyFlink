package com.hnbian.flink.transform

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.metrics.MetricGroup
import org.apache.flink.runtime.checkpoint.CheckpointOptions
import org.apache.flink.runtime.jobgraph.OperatorID
import org.apache.flink.runtime.state.CheckpointStreamFactory
import org.apache.flink.streaming.api.operators.{ChainingStrategy, OneInputStreamOperator, OperatorSnapshotFutures}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.runtime.streamrecord.{LatencyMarker, StreamRecord}

import scala.Byte.MinValue

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2020-07-31 16:33 
  **/
object DataStreamTest {

  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    setMaxParallelism(env)
    env.execute()
  }

  /**
    * 最大并行度可以在设置并行度的地方设定(除了客户端和系统层次)。不同于调用setParallelism()方法， 你可以通过调用setMaxParallelism()方法来设定最大并行度。
    *
    * 默认的最大并行度大概等于‘算子的并行度+算子的并行度/2’，其下限为1而上限为32768。
    *
    * 注意 设置最大并行度到一个非常大的值将会降低性能因为一些状态的后台需要维持内部的数据结构，而这些数据结构将会随着key-groups的数目而扩张（key-groups 是rescalable状态的内部实现机制 ）。
    *
    * @param env
    */
  def setMaxParallelism(env:StreamExecutionEnvironment): Unit ={
    val stream1:DataStream[Integer] = env.fromElements(1,2,3,4)

    stream1.map(v=>v).setMaxParallelism(30000000)
      .print("setMaxParallelism")
  }

  def setParallelism(env:StreamExecutionEnvironment): Unit ={
    val stream1:DataStream[Integer] = env.fromElements(1,2,3,4)
    stream1.map(v=>v).setParallelism(3).print("setParallelism").setParallelism(2)
//    setParallelism:1> 4
//    setParallelism:2> 3
//    setParallelism:2> 1
//    setParallelism:1> 2
  }

  /**
    * 将上游算子的结果发送到本地下游算子
    * 只适用于上游算子实例数与下游算子相同时，
    * 每个上游算子实例将记录发送给下游算子对应的实例。
    * @param env
    */
  def forward(env:StreamExecutionEnvironment): Unit ={
    val stream1:DataStream[Integer] = env.fromElements(1,2,3,4)
    stream1.forward.print("forward").setParallelism(2) // 上游算子实例数大于下游实例数，会抛出异常
    //stream1.print("forward").setParallelism(2)
  }

  /**
    * 可伸缩分区,根据资源使用情况动态调节同一作业的数据分布，
    * 根据物理实例部署时的资源共享情况动态调节数据分布，目的是让数据尽可能的在同一 solt 内流转，以减少网络开销。
    * @param env
    */
  def rescale(env:StreamExecutionEnvironment): Unit ={
    val stream1:DataStream[Integer] = env.fromElements(1,2,3,4)

    val stream2 = stream1.map(v=>v).setParallelism(4)
    stream2.setParallelism(2).rescale.print().setParallelism(4)

//    3> 3
//    1> 4
//    4> 2
//    2> 1
  }

  /**
    * 负载均衡分区（轮询分区）
    * 上游的每个分区数据按照下游分区个数轮询选择一个下游分区发放数据
    * @param env
    */
  def rebalance(env:StreamExecutionEnvironment): Unit ={
    val stream1:DataStream[Integer] = env.fromElements(1,2,3,4)
    stream1.rebalance

    stream1.print("rebalance")
  }


  /**
    * 将数据随机分区发送到下游算子
    * @param env
    */
  def shuffle(env:StreamExecutionEnvironment): Unit ={
    val stream1:DataStream[Integer] = env.fromElements(1,2,3,4,5,6,7,8,9,10)

    stream1.print("stream1")
    stream1.shuffle
    val stream2 = stream1.map(v=>{v+"-map"})
    stream2.print("stream2")

//    stream1:6> 1
//    stream1:7> 2
//    stream1:8> 3
//    stream2:8> 2-map
//    stream2:9> 3-map
//    stream2:7> 1-map
  }
  /**
    * 广播变量
    * 1. 广播变量创建后，它可以运行在集群中的任何function上，而不需要多次传递给集群节点。
    * 2. 不应该修改广播变量，这样才能确保每个节点获取到的值都是一致的。
    * 3. 可以理解为是一个公共的共享变量，我们可以把一个dataset 数据集广播出去，然后不同的task在节点上都能够获取到，这个数据在每个节点上只会存在一份。
    * 4. 如果不使用broadcast，则在每个节点中的每个task中都需要拷贝一份dataset数据集，比较浪费内存(也就是一个节点中可能会存在多份dataset数据)。
    *
    * @param env
    */
  def broadcast(env:StreamExecutionEnvironment): Unit ={
    val stream1:DataStream[Integer] = env.fromElements(1,2,3,4,5,6,7)
    stream1.broadcast()
    stream1.print("broadcast")
  }

  /**
    * 用户指定的uid，该uid的主要目的是用于在job重启时可以再次分配跟之前相同的uid，应该是用于持久保存状态的目的。
    * 指定的ID用于在整个作业中分配相同的操作员ID提交
    * 此ID必须是唯一的 否则，作业提交将失败。
    *
    * @param env
    */
  def uid(env:StreamExecutionEnvironment): Unit ={
    val stream1:DataStream[Integer] = env.fromElements(1,2,3,4,5,6,7)
    stream1.uid("source-1")
    stream1.print("uid")
  }

  /**
    * 设置当前数据流的名称。
    * 绑定到数据源上的 name 属性是为了调试方便，
    * 如果发生一些异常，我们能够通过它快速定位问题发生在哪里。
    * @param env
    */
  def name(env:StreamExecutionEnvironment): Unit ={
    val stream1:DataStream[Integer] = env.fromElements(1,2,3,4,5,6,7)

    stream1.name("stream1")
    stream1.print("name")
  }



  /**
    * 集数据流的分割，使输出值都去到下一个加转换算子的的第一个实例。
    * 请谨慎使用此设置，因为它可能会导致应用程序出现严重的性能瓶颈。
    * @param env
    */
  def global(env:StreamExecutionEnvironment): Unit ={
    val stream1:DataStream[Integer] = env.fromElements(1,2,3,4,5,6,7)
    stream1.print("stream1")

    val stream2:DataStream[Integer] = stream1.global

    stream2.print("global")




//    stream1:10> 6
//    stream1:7> 3
//    stream1:8> 4
//    stream1:11> 7
//    stream1:9> 5
//    stream1:6> 2
//    stream1:5> 1

//    global:1> 1
//    global:1> 2
//    global:1> 3
//    global:1> 4
//    global:1> 5
//    global:1> 6
//    global:1> 7

  }

  /**
    * DataStream 合并
    * 注意 DataStream 中的数据类型要一致 才可以做 union 操作
    * @param env
    */
  def union(env:StreamExecutionEnvironment): Unit ={
    val stream1:DataStream[Integer] = env.fromElements(1,2)
    val stream2:DataStream[Integer] = env.fromElements(3,4)
    val stream3:DataStream[Integer] = env.fromElements(5,6)

    val stream4:DataStream[Integer] = stream1.union(stream2,stream3)

    stream4.print("union")
//    union> 3
//    union> 4
//    union> 5
//    union> 6
//    union> 1
//    union> 2
  }

  def filter(env:StreamExecutionEnvironment): Unit ={

    val stream1:DataStream[Integer] = env.fromElements(1,2,3,4,5,6,7)

    val stream2:DataStream[Integer]= stream1.filter(v => {
      v > 5
    })

    stream2.print("filter")
//    filter> 6
//    filter> 7

  }

  def flatmap(env:StreamExecutionEnvironment): Unit ={
    val stream1:DataStream[String] = env.fromElements("hello world", "flink demo")

    val stream2:DataStream[String]= stream1.flatMap(v => {
      v.split(" ")
    })

    stream2.print("flatMap")
//    flatMap> hello
//    flatMap> world
//    flatMap> flink
//    flatMap> demo

  }

  /**
    * map 算子的使用
    * @param env
    */
  def map(env:StreamExecutionEnvironment): Unit ={

    val stream1:DataStream[String] = env.fromElements("hello", "world")

    val stream2:DataStream[String]= stream1.map(v => {
      v + "-map"
    })

    stream2.print("map")
//    map> hello-map
//    map> world-map
  }
}
