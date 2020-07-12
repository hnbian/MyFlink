package com.hnbian.flink.source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.util.Random

/**
  * @Author haonan.bian
  * @Description 自定义 source 生成随机数
  * @Date 2020-07-11 22:03 
  **/

object SourceTestRandom{
  def main(args: Array[String]): Unit = {

    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.addSource(new SourceRandom())

    stream.print()

    /**
      * 7> 0.3922027730456249
      * 8> 0.6396968318646995
      * 9> 0.4301654052412155
      * 10> 0.8927365619289196
      */


    env.execute()
  }
}

/**
  * 自定义的数据源，生成随机数
  * 需要继承 SourceFunction 接口并实现run、cancel两个方法
  */
class SourceRandom()  extends SourceFunction[Double]{


  // 状态标识，标识 source 是否正在运行
  var isRunning:Boolean = true

  /**
    * 执行当前数据源的方法，生成数据
    */
  override def run(sourceContext: SourceFunction.SourceContext[Double]): Unit = {

    // 初始化一个随机数生成器

    val rand = new Random()

    while(isRunning){
     sourceContext.collect(rand.nextDouble())
      Thread.sleep(500)
    }
  }

  /**
    * 取消当前数据源的方法，停止生成数据
    */
  override def cancel(): Unit = {
    isRunning = false
  }
}
