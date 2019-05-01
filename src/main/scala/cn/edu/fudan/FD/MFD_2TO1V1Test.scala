package cn.edu.fudan.FD

import java.util.Calendar

import cn.edu.fudan.FDRepair.Result
import cn.edu.fudan.tools.{FDRepair, Rdd2DataFrame}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


object MFD_2TO1V1Test {

  def main(args: Array[String]): Unit = {

    val rawTableName = args(0)
    val ruleType = args(1)
    val ruleContent = args(2)
/*    val hdfspath = args(3)
    val schemaString = args(4)
    val tableId = args(5)
    val userId = args(6)*/

    var userId = 1
    var tableId = 2


    println("收到的参数：")
    args.foreach(x=>println(x))
    var tableName = userId+"_"+tableId+"_"+rawTableName
    tableName = tableName.replaceAll("[^0-9a-zA-Z]","_")



    val conf = new SparkConf().setAppName("MultiFDRepair")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    val sessionCtx = SparkSession.builder().appName("MultiFDRepair").getOrCreate()


    //var ruleContent = "aa,bb,cc->dd;cc,ee->ff;ff,dd->gg;aa->cc;kk,hh->cc"

    val tuple = fun1(ruleContent)             //分解规则，得到map(String,List(ArrayBuffer(String))) 和 Status(String,String)
    val rules = tuple._1

    var setsForSelect:mutable.Set[String]=mutable.Set()
    for( tt <- rules.iterator){
      setsForSelect.add(tt._1)
      for(temp <- tt._2.flatMap(x=>x)){
        setsForSelect.add(temp.toString)
      }
    }
    println("set里的元素是: ")
    setsForSelect.foreach(x=>println(x))
    var simplifyRead = ""
    var tempArr = setsForSelect.toArray
    for( i <- 0 until(tempArr.length)){
      if(i!=tempArr.length-1)
        simplifyRead+=tempArr(i)+","
      else simplifyRead+=tempArr(i)+" "
    }

    val readSql = "select * from sparkdb."+rawTableName
   // val readSql = "select "+simplifyRead+" from "+tableName
    val originalDF = sqlContext.sql(readSql)
    //val originalDF = Rdd2DataFrame.RddTODF(schemaString,hdfspath,sc,sqlContext)

    var schemaa = originalDF.schema.add("VISITED","string",true)

    val tempDF1 = originalDF.rdd.map(row =>{
      var seq = row.toSeq
      seq = seq :+ "0"
      Row.fromSeq(seq)
    })

    var exeDF = sessionCtx.createDataFrame(tempDF1,schemaa)









    var statusMap = tuple._2

    println("初始的status：")
    statusMap.iterator.foreach(x=>println(x._1+":"+x._2))


    var judgeCycle = statusMap.count(x=>"0".equals(x._2))
    println("需要做的修复数量（状态为0的节点）："+judgeCycle)
    var count =0

    var flag=true
    while (flag){
      val mapTuple = nextRepair(rules,statusMap)
      if(mapTuple == null)  flag=false

      else {
        println("next的map")
        println(mapTuple._1,mapTuple._2.flatMap(x=>x),mapTuple._3)

        if("1".equals(mapTuple._3)){
          exeDF = FDRepair.doEqualClassRepair(exeDF,mapTuple._2,mapTuple._1,sqlContext)
        }
        else {
          exeDF = FDRepair.doMFDRepair(exeDF,mapTuple._2,mapTuple._1,sqlContext)
        }

        println("里面打印status")
        statusMap.iterator.foreach(x=>println(x._1+":"+x._2))
        statusMap(mapTuple._1)="1"
        println("里面打印修改后sttus")
        statusMap.iterator.foreach(x=>println(x._1+":"+x._2))

        count+=1
        println("已经修复了："+count+" 次")

      }

      if(!flag&&count!=judgeCycle){
        println("规则中存在环，无法继续修复。")
      }
    }

    println("最终结果")
    exeDF.show()

    val dirty = exeDF.filter("VISITED = '1'").count()
    val total = originalDF.count()

    val writebackDF = exeDF.drop("VISITED")

    writebackDF.registerTempTable("repairResult")

    val stat = Seq(Result(dirty, total, ruleContent))
    val tempDF = sqlContext.createDataFrame(stat)
    tempDF.show()
    tempDF.registerTempTable("temp")

    val tableName1 = tableName + "_" + ruleType + "_" + System.currentTimeMillis
    println("start writing back result at " + Calendar.getInstance().getTime)

    println("TableName: "+tableName1)
    sqlContext.sql("CREATE TABLE IF NOT EXISTS sparkrepair." + tableName1 + " as select * from repairResult")
    sqlContext.sql("CREATE TABLE IF NOT EXISTS sparkdb." + tableName1 + " as select * from temp")

    println("finish detecting and writing back result at " + Calendar.getInstance().getTime)

    println("SuCcess")

  }

  def fun1(origContent:String):(mutable.Map[String,List[ArrayBuffer[String]]],mutable.Map[String,String])={
    var origArray = origContent.replaceAll("\\s+","").split(";")
    var resultMap:mutable.Map[String,List[ArrayBuffer[String]]] = mutable.Map()
    var statusMap:mutable.Map[String,String] = mutable.Map()

    for(oa <- origArray){
      var tt = oa.split("->")
      var arry:ArrayBuffer[String] = ArrayBuffer()
      var leftAttr = tt(0).split(",")
      for(t <- leftAttr){
        arry+=t
        if(!statusMap.contains(t)){
          statusMap(t)="1"
        }
      }
      statusMap(tt(1))="0"
      if(resultMap.contains(tt(1))){
        resultMap(tt(1))=resultMap(tt(1)):+arry
      }
      else{
        resultMap(tt(1))=List(arry)
      }

    }
/*
    for(m <- resultMap.iterator){
         println(m._1+" : "+m._2.flatMap(x=>x))
         println(m._1+" : "+m._2)
    }
    for(m <- statusMap.iterator){
      println(m._1+ ":"+m._2)
    }
*/
    (resultMap,statusMap)
  }

  def nextRepair(mapSet:mutable.Map[String,List[ArrayBuffer[String]]],statusMap:mutable.Map[String,String]
                ):(String,List[ArrayBuffer[String]],String)={

    for(map <- mapSet){
      var flag = "0"
      if(map._2.length>1) flag= "1"
      val lefts = map._2.flatMap(x=>x)
      if("0".equals(statusMap(map._1))&&lefts.forall(x=>"1".equals(statusMap(x)))){
        return (map._1,map._2,flag)
      }
    }
    null
  }
}
