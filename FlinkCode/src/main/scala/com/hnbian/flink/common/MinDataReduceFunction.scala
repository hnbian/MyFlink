package com.hnbian.flink.common

import org.apache.flink.api.common.functions.ReduceFunction

/**
  * @Author haonan.bian
  * @Description //TODO
  * @Date 2021/1/3 14:22 
  **/
class MinDataReduceFunction extends ReduceFunction[Obj1]{

  override def reduce(r1: Obj1, r2: Obj1):Obj1 = {
    println(r1.toString)
    if(r1.time > r2.time){
      r1
    }else{
      r2
    }
  }
}
