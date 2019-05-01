package cn.edu.fudan

/**
  * Created by FengSi on 2017/06/07 at 20:06.
  */
object Test {
  def main(args: Array[String]): Unit = {
    println("测试scala jdk是否正常")
    var a: Map[(String,String), String] = Map()
    println(a)
    a += (("a","b") -> "c")
    println(a)
    println(a(("a","b")))
    println(a.get(("a","c")))
  }
}
