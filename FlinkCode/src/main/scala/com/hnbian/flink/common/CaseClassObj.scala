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
1,小红0,20 触发 1 触发 2
2,xx,100
1,小红1,20 触发 1 触发 2
1,小红2,20 触发 2
2,xx,200 触发 1>
          start:Record(1,小红0,20),end:Record(2,xx,200)
          start:Record(1,小红1,20),end:Record(2,xx,200)

1,小红3,20 触发 2 触发 3
1,小红4,20 触发 2 触发 3
1,小红5,20 触发 3 触发 4
2,xx,300 触发 2>
          start:Record(1,小红0,20),end:Record(2,xx,300)
          start:Record(1,小红2,20),end:Record(2,xx,300)
          start:Record(1,小红1,20),end:Record(2,xx,300)
          start:Record(1,小红4,20),end:Record(2,xx,300)
          start:Record(1,小红3,20),end:Record(2,xx,300)

1,小红6,20 触发 3 触发 4 触发 5
1,小红7,20 触发 3 触发 4 触发 5
1,小红8,20 触发 3 触发 4 触发 5
1,小红9,20 触发 4 触发 5
2,xx,400 触发 3 >
          start:Record(1,小红8,20),end:Record(2,xx,400)
          start:Record(1,小红6,20),end:Record(2,xx,400)
          start:Record(1,小红5,20),end:Record(2,xx,400)
          start:Record(1,小红3,20),end:Record(2,xx,400)
          start:Record(1,小红4,20),end:Record(2,xx,400)
          start:Record(1,小红7,20),end:Record(2,xx,400)

1,小红10,20 触发 4 触发 5
1,小红11,20 触发 5
2,xx,400 触发 4 >
            start:Record(1,小红7,20),end:Record(2,xx,400)
            start:Record(1,小红10,20),end:Record(2,xx,400)
            start:Record(1,小红8,20),end:Record(2,xx,400)
            start:Record(1,小红5,20),end:Record(2,xx,400)
            start:Record(1,小红6,20),end:Record(2,xx,400)
            start:Record(1,小红9,20),end:Record(2,xx,400)

1,小红12,20
2,xx,500 触发 5>
          start:Record(1,小红10,20),end:Record(2,xx,500)
          start:Record(1,小红9,20),end:Record(2,xx,500)
          start:Record(1,小红8,20),end:Record(2,xx,500)
          start:Record(1,小红7,20),end:Record(2,xx,500)
          start:Record(1,小红11,20),end:Record(2,xx,500)
          start:Record(1,小红6,20),end:Record(2,xx,500)

2,xx,600 未触发
*/




/**
1,小红0,20 -》触发 1
2,xx,100 未触发，因为 age==20  只累积一次

1,小红1,20 -》触发 1
1,小红2,20 -》触发2
2,xx,200 触发1 》 Record(1,小红1,20),Record(2,xx,200)
                Record(1,小红0,20),Record(2,xx,200)

1,小红3,20 -》触发2
1,小红4,20 -》触发2
1,小红5,20 -》触发 3 前累积5 次 超过最大值的 4次，故丢弃该数据
2,xx,300 触发2 》 Record(1,小红3,20),Record(2,xx,300)
                Record(1,小红2,20),Record(2,xx,300)
                Record(1,小红4,20),Record(2,xx,300)

1,小红6,20 触发3
1,小红7,20 触发3
1,小红8,20 触发3
1,小红9,20 -》触发 3 前累积7 次 超过最大值的 4次，故丢弃该数据
2,xx,400 触发3 》Record(1,小红6,20),Record(2,xx,400)
                Record(1,小红8,20),Record(2,xx,400)
                Record(1,小红7,20),Record(2,xx,400)

1,小红10,20 -》触发 3 前累积7 次 超过最大值的 4次，故丢弃该数据
1,小红11,20 触发4
1,小红12,20 触发4
1,小红13,20 触发4
1,小红14,20 未触发
2,xx,400 触发4 》Record(1,小红11,20),Record(2,xx,400)
                Record(1,小红12,20),Record(2,xx,400)
                Record(1,小红13,20),Record(2,xx,400)
2,xx,500 未触发





  */



/*CepQuantifierTimesTest:6> Record(1,小明5,20),Record(2,小小小小小小,2)
CepQuantifierTimesTest:8> Record(1,小红9,20),Record(2,小小小小小小,2)
CepQuantifierTimesTest:7> Record(1,小红8,20),Record(2,小小小小小小,2)

CepQuantifierTimesTest:9> Record(1,小红7,20),Record(2,小小小小小小,3)
CepQuantifierTimesTest:11> Record(1,小红2,20),Record(2,小小小小小小,3)
CepQuantifierTimesTest:10> Record(1,小明6,20),Record(2,小小小小小小,3)

CepQuantifierTimesTest:1> Record(1,小红1,20),Record(2,小小小小小小,4)
CepQuantifierTimesTest:2> Record(1,小明2,20),Record(2,小小小小小小,4)
CepQuantifierTimesTest:12> Record(1,小明4,20),Record(2,小小小小小小,4)*/


/*
1,小红1,20
1,小红2,20
2,小小小小小小,22 》 Record(1,小红1,20),Record(2,小小小小小小,22)
1,小明3,20
1,小红4,20
1,小红5,20
2,小小小小小小,22 》Record(1,小红4,20),Record(2,小小小小小小,22)
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



