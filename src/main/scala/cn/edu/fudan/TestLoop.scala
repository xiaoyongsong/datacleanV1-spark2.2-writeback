package cn.edu.fudan

import scala.tools.nsc.interpreter.ILoop
import scala.tools.nsc.Settings
/**
  * Created by FengSi on 2017/07/25 at 21:39.
  */
object TestLoop {
  def main(args: Array[String]): Unit = {
    val loop = new ILoop
    loop.process(new Settings)
  }
}
