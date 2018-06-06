package com.ljy.day10

object MyPref {
  //第一种排序方式
  implicit val girlOrdering = new Ordering[Girl] {
    override def compare(x: Girl, y: Girl): Int = {
      if (x.face==y.face){
        x.age-y.age
      }else{
        y.face - x.face
      }
    }
  }
}
