package com.hnbian.flink.common

case class Student(id:String,name:String,classId:String)

/**
1,小明,1001
2,小红,1002
  */
case class Class(id:String,name:String)

/**
1001,一班
1002,二班
  */


case class Record(classId:String,name:String,age:Int)

/**
1,小明,20
1,小红1,20
1,小红2,20
1,小明3,20
1,小红4,20
1,小红5,20
  */




case class Obj1(id:String,name:String,time:Long)

/**
1,小明,1598018502
1,小红,1598018503
1,小明,1598018504
1,小红,1598018505
3,小明,1598018506
4,小红,1598018507
3,小明,1598018508
2,小红,1598018511
1,小明,1598018514
2,小红,1598018500
3,小明,1598018502
2,小红,1598018503
3,小明,1598018504
2,小红,1598018505
2,小明,1598018506
4,小红,1598018507
2,小明,1598018508
3,小红,1598018511
3,小明,1598018514
  */



