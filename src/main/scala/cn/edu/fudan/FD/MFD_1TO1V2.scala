package cn.edu.fudan.FD

import java.util.Calendar

import cn.edu.fudan.FDRepair.Result
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.Map

/**
  * 该版本更改dirty的计算方式，按冲突对来算  （1 2）（1 2）（1 3）算3
  */

object MFD_1TO1V2 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("FD_2TO1")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    // val args = Array("test","fd","city_zip")
    val tableName = args(0).replaceAll("\\s+","")
    val ruleType = args(1)
    val ruleContent = args(2).replaceAll("\\s+","")

    var originalSelect = ""

    val orderedList = getOrder(ruleContent)

    var set:mutable.Set[String] = mutable.Set()
    for (od <- orderedList){
      set+=od._1
      set+=od._2
    }
    var array = set.toArray
    for( i<-array.indices){
      if(i!=array.length-1){
        originalSelect+=array(i)+","
      }
      else{
        originalSelect+=array(i)+" "
      }
    }

    println("originalAttr: "+originalSelect)

    val readSql = "select * from default."+tableName

    var originalDF = sqlContext.sql(readSql)
    println("读取的sql语句是："+readSql)

    var schemaa = originalDF.schema.add("VISITED","string",true)

    var exeDF = sqlContext.createDataFrame(originalDF.rdd.map(row =>{
      var seq = row.toSeq
      seq = seq :+ "0"
      Row.fromSeq(seq)
    }),schemaa)

    for(pair <- orderedList){
      exeDF = oneRepair(exeDF,pair,sqlContext,sc)
      exeDF.cache()
    }

    val dirty = exeDF.select("VISITED").filter("VISITED = '1'").count()
    val total = exeDF.count()

    println("dirty:"+dirty+"  total:"+total)
    val rebackDF = exeDF.drop("VISITED")

    println("修复后写回的dataFrame：")
    // rebackDF.show()

    rebackDF.registerTempTable("repairResult")
    sqlContext.sql("select "+originalSelect+" from repairResult").show()

    // rebackDF.select("zip,city,state").orderBy("zip").show()

    val stat = Seq(Result(dirty, total, ruleContent))
    val tempDF = sqlContext.createDataFrame(stat)
    tempDF.show()
    tempDF.registerTempTable("temp")

    val tableName1 = tableName + "_" + ruleType + "_" + System.currentTimeMillis
    println("start writing back result at " + Calendar.getInstance().getTime)

    println("TableName: "+tableName1)
    //sqlContext.sql("CREATE TABLE IF NOT EXISTS sparkrepair." + tableName1 + " as select * from repairResult")
    //sqlContext.sql("CREATE TABLE IF NOT EXISTS sparkdb." + tableName1 + " as select * from temp")

    println("finish detecting and writing back result at " + Calendar.getInstance().getTime)

  }

  def getOrder(originalStr:String):List[(String,String)]={

    val fdPairs = originalStr.replaceAll("\\s+","").split(";")

    var inMap:Map[String,Int] = Map()
    var outMap:Map[String,Int] = Map()
    var pairList:List[(String,String)] = List()

    for( pair <- fdPairs){
      val tempPair = pair.split("->")
      val leftAttr = tempPair(0)
      val rightAttr = tempPair(1)
      pairList = pairList:+Tuple2(leftAttr,rightAttr)
      inMap(leftAttr)=0
      inMap(rightAttr)=0
      outMap(leftAttr)=0
      outMap(rightAttr)=0
    }

    for( pairTuple <- pairList){
      outMap(pairTuple._1) = outMap(pairTuple._1)+1
      inMap(pairTuple._2) = inMap(pairTuple._2)+1
    }

    var finalList:List[(String,String)] = List()

    while(inMap.count(x => (x._2 == 0) && (outMap(x._1) != 0))>0) {

      val zeros = inMap.filter(x => (x._2 == 0)&&(outMap(x._1)!=0))
      zeros.foreach(x=>println("入度为零的FD："+x._1+":"+x._2))
      for (zero <- zeros) {
        for (tem <- pairList) {
          if (tem._1.equals(zero._1)) {
            finalList = finalList :+ tem
            inMap(tem._2) -= 1
            inMap(zero._1) = -1
          }
        }
      }
    }
    finalList.foreach(x=>println("拓扑排序："+x._1+"->"+x._2))

    finalList
  }

  def oneRepair(originalDF:DataFrame,ruleContent:(String,String),sqlContext:HiveContext,sc:SparkContext):DataFrame= {

    val leftAttr = ruleContent._1
    val rightAttr = ruleContent._2
    val df = originalDF.select(leftAttr, rightAttr)

    /* println("select后的数据:")
     df.show()*/

    val df1 = df.rollup(leftAttr, rightAttr).count()

    val df11 = df1.filter(df1(leftAttr).isNotNull)
    val df22 = df11.filter(df11(rightAttr).isNotNull)

    val df33 = df11.filter(df11(rightAttr).isNull).withColumnRenamed("count", "count_").
      withColumnRenamed(leftAttr, leftAttr + "_").withColumnRenamed(rightAttr, rightAttr + "_")

    val vio = df22.join(df33, df22(leftAttr) === df33(leftAttr + "_")).filter(df22("count")<df33("count_")&&
      df22("count") > (df33("count_")/2)).select(leftAttr,rightAttr)

    println("vio不去重："+vio.count())
    println("vio去重大小:"+vio.distinct().count())

    val repairMap:Map[String,String]= Map()

    vio.collect().foreach(row =>{
      repairMap += (row.getString(0) -> row.getString(1))
    } )

    /*println("修复Map")
    for(rep<-repairMap){
      println(rep._1+"修改为"+rep._2)
    }*/

    println("修复Map的size:"+repairMap.size)

    val accum = sc.accumulator(0)

    def audit(row: Row, cor: Map[String, String]): Row = {
      val left = row.fieldIndex(leftAttr)
      val right = row.fieldIndex(rightAttr)
      val visitedIndex = row.fieldIndex("VISITED")
      var value = row.toSeq
      if (cor.contains(value(left).asInstanceOf[String])){

        value=value.updated(visitedIndex,"1")
        accum.add(1)
        if(!value(right).asInstanceOf[String].equals(cor(value(left).asInstanceOf[String]))){
          val b = value.updated(right, cor(value(left).asInstanceOf[String]))
          Row.fromSeq(b)
        }
        else {
          Row.fromSeq(value)
        }
      }
      else {
        row
      }
    }
    val re = sqlContext.createDataFrame(originalDF.rdd.map(row => audit(row, repairMap)), originalDF.schema)
    println("accum:"+accum.value)

    re
  }

}
