package cn.edu.fudan

import java.util.regex.Pattern

import cn.edu.fudan.DifferDependency.DD.fun2
import org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.ArrayBuffer

object xysTest {

  def main(args: Array[String]): Unit = {

    /*val conf = new SparkConf().setAppName("produceData")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    val tableName = args(0)
    val ruleType = args(1)
    val ruleContent = args(2)

    val df = sqlContext.sql("select zip,city,state,phone,salary,areacode from wonders_dataqualitytest")*/

    println("sasa">"sasaff")





  }

  def fun1(ruleContent:String) ={
    val lrrules = ruleContent.split("@")
    var leftr = lrrules(0).split(",")
    var rightr = lrrules(1).split(",")

    leftr = leftr.map(x=>x.trim())
    rightr = rightr.map(x=>x.trim())

    val leftArray = fun2(leftr,0)
    val riggyArray = fun2(rightr,1)


    (leftArray,riggyArray)

  }

  def fun2(arrayRule:Array[String],flag:Int): ArrayBuffer[Map[String,String]] ={

    var arryBuffer = ArrayBuffer[Map[String,String]]()
    for(rule<-arrayRule){
      var map:Map[String,String] = Map()
      val attr = rule.replaceAll("[0-9<>=]","")
      var opr = rule.replaceAll("[^<>=]","")
      var pat= Pattern.compile("(^[0-9]+)|([0-9]+$)")
      val matchs =pat.matcher(rule)
      matchs.find()
      val number = matchs.group()
      map += ("attr"->attr)
      map += ("val"->number)

      if(flag==1){
        if("<=".equals(opr)) opr=">"
        else if (">=".equals(opr)) opr="<"
        else if (">".equals(opr)) opr = "<="
        else if ("<".equals(opr)) opr = ">="
        else opr = "!="
      }
      map += ("op"->opr)
      arryBuffer+=map

    }
    return arryBuffer

  }
}
