package com.bala.scala
object Sample_programs {
  def main (args : Array[String]): Unit ={
    def facto(n:Int):Int ={
      var a = 1
      for(i <- n to 1 by -1)
        a= a*i
        a
    }
    println(facto(5))
  }
}
