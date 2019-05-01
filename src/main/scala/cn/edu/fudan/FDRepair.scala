package cn.edu.fudan

import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by FengSi on 2017/09/21 at 12:07.
  */
object FDRepair {
  case class Result(dirty: Long, total: Long, rule: String)
  def main(args: Array[String]): Unit = {

    val tableName = args(0)
    val ruleType = args(1)
    val ruleContent = args(2)

    val leftAttr = ruleContent.split("_")(0).trim
    val rightAttr = ruleContent.split("_")(1).trim

    val conf = new SparkConf().setAppName("FunctionDependencyRepair")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    //    val args = Array("quality_hospital500k", "zip", "city", "fd", "gfgdgfd")
    println("start detecting table: "+ tableName +" at " + Calendar.getInstance().getTime)


    var originalSql = "select "+leftAttr+","+rightAttr+" from " +tableName

    val rowDf = sqlContext.sql("select * from "+tableName)

    //val df = sqlContext.sql(originalSql)

    val df = rowDf.select(leftAttr,rightAttr)
    println("读取数据的sql语句是："+originalSql)
    df.persist()

    var total = df.count()

    // test

    val al = df.rollup(leftAttr, rightAttr).count
    al.persist()
    val all = al.filter(al(leftAttr).isNotNull)
    val a1 = all.filter(all(rightAttr).isNotNull)
    val a2 = all.filter(all(rightAttr).isNull).withColumnRenamed(leftAttr, leftAttr + "_").
      withColumnRenamed(rightAttr, rightAttr + "_").withColumnRenamed("count", "count_")
    val repair = a1.join(a2, a1(leftAttr) === a2(leftAttr + "_")).
      filter(a1("count") < a2("count_") && a1("count") > a2("count_")/2).select(leftAttr, rightAttr)

    println("repari大小："+repair.count())
    var cor: Map[String,String] = Map()

    //必须先收集到driver端才可产生正确的map
    repair.collect.foreach(row => cor += (row.getString(0) -> row.getString(1)))
    //repair.foreach(row => cor += (row.getString(0) -> row.getString(1)))   错误

    def audit(row: Row, cor: Map[String, String]): Row = {
      val left = row.fieldIndex(leftAttr)
      val right = row.fieldIndex(rightAttr)
      val value = row.toSeq
      if (cor.contains(value(left).asInstanceOf[String]) &&
        !value(right).asInstanceOf[String].equals(cor(value(left).asInstanceOf[String]))) {
        val b = value.updated(right, cor(value(left).asInstanceOf[String]))
        Row.fromSeq(b)
      } else {
        row
      }
    }

    val re = sqlContext.createDataFrame(rowDf.rdd.map(row => audit(row, cor)), rowDf.schema)

    var dirty=0L
    //xys
     println("map大小："+cor.size)

    if(repair.count() > 0  ){
      dirty = rowDf.join(repair,leftAttr).count()
    }

    val stat = Seq(Result(dirty, total, ruleContent))
    val tempDF = sqlContext.createDataFrame(stat)

    tempDF.show()
    tempDF.registerTempTable("temp")


    //    val re = df.map(row => audit(row, cor))
    // test

    /*
    val violations = df.rollup(leftAttr, args(2)).count.groupBy(args(1)).count.
      filter("count > 2").sort(args(1)).select(args(1)).withColumnRenamed(leftAttr, leftAttr + "_")

    val v = df.join(violations, df(args(1)) === violations(leftAttr + "_")).drop(leftAttr + "_")
    v.count
    println("dirty data's number is " + v.count())
    println("total data's number is " + df.count())
    println("part of the dirty data are ")

    val violationss = re.rollup(leftAttr, args(2)).count.groupBy(args(1)).count.
      filter("count > 2").sort(args(1)).select(args(1)).withColumnRenamed(leftAttr, leftAttr + "_")

    val vv = re.join(violationss, re(args(1)) === violationss(leftAttr + "_")).drop(leftAttr + "_")
    println("dirty data's number is " + vv.count())
    println("total data's number is " + re.count())

    //防止沒有錯誤的時候表為空，
    val stat = Seq(Result(v.count(), df.count(), args(4)))
    val ddf = sqlContext.createDataFrame(stat)

    val result = v.join(ddf)
    result.registerTempTable("result")

    */

    re.registerTempTable("repairResult")

    val tableName1 = tableName + "_" + ruleType + "_" + System.currentTimeMillis

    println("start writing back result at " + Calendar.getInstance().getTime)

    sqlContext.sql("CREATE TABLE IF NOT EXISTS sparkrepair." + tableName1 + " as select * from repairResult")
    sqlContext.sql("CREATE TABLE IF NOT EXISTS sparkdb." + tableName1 + " as select * from temp")

    println("finish detecting and writing back result at " + Calendar.getInstance().getTime)
  }
}
