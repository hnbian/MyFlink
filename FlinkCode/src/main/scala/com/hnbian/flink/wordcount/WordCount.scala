package com.hnbian.flink.wordcount

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._

/**
  * @Author haonan.bian
  * @Description 第一个Wordcount代码
  * @Date 2020-04-28 22:31 
  **/
object WordCount extends App {

  var env:ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment


  val path = "/Users/hnbian/Documents/GitHub/FlinkCodes/FlinkCode/src/main/resources/words.txt"

  val intputDataSet = env.readTextFile(path)

  val list = intputDataSet.flatMap(_.toLowerCase.split(" "))
    .filter(_.length > 2)
    .map((_, 1))
    .groupBy(0)
    .sum(1)
    .sortPartition(1, Order.DESCENDING) // 分区内排序
    .print()

  /**
    * (xiaom,1)
    * (xiaod,1)
    * (xiaog,1)
    * (xiah,1)
    * (hello,4)
    */
}
