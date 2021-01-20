package com.hnbian.flink.common

import org.apache.flink.api.common.typeinfo.TypeInformation

case class Record(classId:String,name:String,age:Int)

case class Obj1(id:String,name:String,time:Long)

/**
  * 1,小明,1598018502
  * 1,小红,1598018503
  * 1,小明,1598018504
  * 1,小红,1598018505
  * 3,小明,1598018506
  * 4,小红,1598018507
  * 3,小明,1598018508
  * 2,小红,1598018511
  * 1,小明,1598018514
  * 2,小红,1598018500
  * 3,小明,1598018502
  * 2,小红,1598018503
  * 3,小明,1598018504
  * 2,小红,1598018505
  * 2,小明,1598018506
  * 4,小红,1598018507
  * 2,小明,1598018508
  * 3,小红,1598018511
  * 3,小明,1598018514
  */



