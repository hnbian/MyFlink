package com.hnbian.flink.wordcount

import org.apache.flink.api.scala._

/**
  * @Author haonan.bian
  * @Description 第一个Wordcount代码
  * @Date 2020-04-28 22:31 
  **/
object WordCount  extends App{

  val env = ExecutionEnvironment.getExecutionEnvironment

  val path = "/Users/hnbian/Documents/GitHub/FlinkCodes/FlinkCode/src/main/resources/words.txt"

  val intputDataSet = env.readTextFile(path)

  intputDataSet.flatMap(_.split(" "))
    .map((_,1))
    .groupBy(0)
    .sum(1)
    .print()

  /**
    * (xiaom,1)
    * (xiaod,1)
    * (xiaog,1)
    * (xiah,1)
    * (hello,4)
    */
}
