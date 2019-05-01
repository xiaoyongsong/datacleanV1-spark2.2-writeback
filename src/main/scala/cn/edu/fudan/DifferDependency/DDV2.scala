package cn.edu.fudan.DifferDependency

import java.util.regex.{Pattern}

import cn.edu.fudan.Result
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.ArrayBuffer

object DDV2 {

  /**
    *
    *  此版本是把HQL改为sparksql版
    */
  def main(args: Array[String]): Unit = {

    var startForProgram = System.nanoTime()

   // val args = Array("wonders_dataqualitytest", "DD", "salary<10000,salary>8000@zip<10000,leve>10")
    val conf = new SparkConf().setAppName("DDDecte2")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    val tableName = args(0)
    val ruleType = args(1)
    val ruleContent = args(2).replaceAll("\\s+","")   //准备事先去掉内容中的空格，然而在linux命令行中，有空格就当多个参数处理了，先这样吧

    println("tableName: "+tableName+",ruleType: "+ruleType+" ruleContenr: "+ruleContent)

    val tup = fun1(ruleContent)
    val leftArray = tup._1
    val rightArray = tup._2

    var whereSql = ""
    var testWhere= ""


    var attr =""
    var number = ""
    var opr = ""
    var lset = Set[String]()       //为了去重用SET，属性去重，用在sql的select中
    var rset = Set[String]()
    for( i <- 0 until leftArray.length){

      attr = leftArray(i)("attribute")
      number = leftArray(i)("number")
      opr = leftArray(i)("operator")
      lset+=attr  //存储左边的属性集合，用于select

      println(attr+"---"+opr+"----"+number)
      if(i!=leftArray.length-1){
        whereSql+="ABS(A."+attr+"-B."+attr+")"+opr+number+" AND "
        testWhere+="(("+attr+"-"+attr+"_)"+opr+number+" OR "+"("+attr+"_"+"-"+attr+")"+opr+number +") AND "
      }
      else{
        whereSql+="ABS(A."+attr+"-B."+attr+")"+opr+number+" AND ("
        testWhere+="(("+attr+"-"+attr+"_)"+opr+number+" OR "+"("+attr+"_"+"-"+attr+")"+opr+number +") AND ("
      }
    }


    for (i <- 0 until rightArray.length){

      attr = rightArray(i)("attribute")
      number = rightArray(i)("number")
      opr = rightArray(i)("operator")

      rset+=attr             //存储右边的属性集合，用于select
      if(i!=rightArray.length-1){
        whereSql+="ABS(A."+attr+"-B."+attr+")"+opr+number+" OR "
        testWhere+="("+attr+"-"+attr+"_)"+opr+number+" OR "+"("+attr+"_"+"-"+attr+")"+opr+number +" OR "

      }
      else{
        whereSql+="ABS(A."+attr+"-B."+attr+")"+opr+number+") "
        testWhere+="("+attr+"-"+attr+"_)"+opr+number+" OR "+"("+attr+"_"+"-"+attr+")"+opr+number+") "
      }
    }
    var tempLeftAtr = lset.toList
    var tempRightAtr = rset.toList
    var selected = ""
    var originalSelected=""

    var testOriginalSelect = ArrayBuffer[String]()

    for(i <- 0 until tempLeftAtr.length){

      var attr = tempLeftAtr(i)
      if(i!=tempLeftAtr.length-1){
        selected+="A."+attr+" AS A_"+attr+",B."+attr+" AS B_"+attr+","
        originalSelected+=attr+","
        testOriginalSelect+=attr
      }
      else{
        selected+="A."+attr+" AS A_"+attr+",B."+attr+" AS B_"+attr+", "
        originalSelected+=attr+", "
        testOriginalSelect+=attr
      }
    }
    for(i <- 0 until tempRightAtr.length){

      var attr = tempRightAtr(i)
      if(i!=tempRightAtr.length-1){
        selected+="A."+attr+" AS A_"+attr+",B."+attr+" AS B_"+attr+","
        originalSelected+=attr+","

        testOriginalSelect+=attr
      }
      else{
        selected+="A."+attr+" AS A_"+attr+",B."+attr+" AS B_"+attr+" "
        originalSelected+=attr+" "

        testOriginalSelect+=attr
      }
    }

    val originalSql = "select "+originalSelected+"from default."+tableName
    println("读取数据库的sql语句："+originalSql)

    var startForRead = System.nanoTime()
    val dd = sqlContext.sql(originalSql)
    var endForRead = System.nanoTime()
    println("读取数据耗时："+(endForRead-startForRead)/1000000000+"s")


    var dd2 = dd.toDF()

    for( i <- 0 until(testOriginalSelect.length)){
      var tempstr = testOriginalSelect(i)
      println("列名替换："+tempstr+"->"+tempstr+"_")
      dd2 = dd2.withColumnRenamed(tempstr,tempstr+"_")
    }

    var columns = dd2.columns

    for(str <- columns){
      println("更新的表名："+str)
    }

    println("filter式子："+testWhere)

    var total = dd.count()
    var dirty = dd.join(dd2).where(testWhere).count()
    val stat = Seq(Result(dirty/2, total*(total-1)/2, ruleContent))
    val tempDF = sqlContext.createDataFrame(stat)
    tempDF.registerTempTable("result")
    tempDF.show()

   /* var endForProgram = System.nanoTime()
    println("程序总耗时："+(endForProgram-startForProgram)/1000000000+"s")

    val newTableName1 = tableName + "_" + ruleType + "_" + System.currentTimeMillis
    val newTableName=newTableName1.trim()*/



    /*
    var dd1 = dd.toDF()
    val total = dd.count()   //表的总数

    dd1.persist()
    dd.persist()
    dd.registerTempTable("A")
    dd1.registerTempTable("B")

    //val executeSql = "select "+selected+" from A,B WHERE "+whereSql
    val executeSql = "select count(*) from A,B WHERE "+whereSql
    println("执行sql是: "+executeSql)
    var resultDF = sqlContext.sql(executeSql)

    val dirty = Integer.parseInt(resultDF.first()(0).toString)
    val stat = Seq(Result(dirty/2, total*(total-1)/2, ruleContent))
    val tempDF = sqlContext.createDataFrame(stat)
    tempDF.registerTempTable("result")
    tempDF.show()
    //resultDF.show()

    val newTableName1 = tableName + "_" + ruleType + "_" + System.currentTimeMillis
    val newTableName=newTableName1.trim()

    //resultDF.registerTempTable("result")
    // sqlContext.sql("CREATE TABLE IF NOT EXISTS sparkdb." + newTableName + " as select * from result")

    val writeBackSql = "CREATE TABLE IF NOT EXISTS sparkdb."+newTableName+" as select * from result"
    sqlContext.sql(writeBackSql)
    println("写回的sql为："+writeBackSql)

    var endForProgram = System.nanoTime()
    println("程序总耗时："+(endForProgram-startForProgram)/1000000000+"s")

*/
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
      // val opr = rule.replaceAll("[a-zA-Z<>=]","")
      var pat= Pattern.compile("(^[0-9]+)|([0-9]+$)")
      val matchs =pat.matcher(rule)
      matchs.find()
      val number = matchs.group()
      map += ("attribute"->attr)
      map += ("number"->number)

      if(flag==1){
        if("<=".equals(opr)) opr=">"
        else if (">=".equals(opr)) opr="<"
        else if (">".equals(opr)) opr = "<="
        else if ("<".equals(opr)) opr = ">="
        else opr = "!="
      }
      map += ("operator"->opr)
      arryBuffer+=map

    }
    return arryBuffer

  }


}
