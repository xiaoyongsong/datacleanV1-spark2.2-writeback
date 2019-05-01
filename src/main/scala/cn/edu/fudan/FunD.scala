package cn.edu.fudan

import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by FengSi on 2017/07/16 at 11:14.
  */
object FunD {
  def main(args: Array[String]): Unit = {

    var startForProgram = System.nanoTime()

    val conf = new SparkConf().setAppName("FunctionDependency")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

//    val args = Array("wonders_dataqualitytest", "zip", "state", "fd", "gfgdgfd", "gfdg")
    //val args = Array("wonders_dataqualitytest","zip","state","fd","zip|state","FD")

    val tableName = args(0)
    val ruleType = args(1)
    val ruleContent = args(2)

    println("名字："+tableName+" 规则："+ruleType+" 内容: "+ruleContent)

    val leftField = ruleContent.split("_")(0).trim
    val rightField = ruleContent.split("_")(1).trim

    println("start detecting table: " + tableName + " at " + Calendar.getInstance().getTime)

    //val sqlStr = "SELECT * FROM default." + tableName
    val sqlStr = "SELECT "+leftField+","+rightField+" FROM default." + tableName

    println("读取的sql语句是： "+sqlStr)

    var startForRead = System.nanoTime()
    val df = sqlContext.sql(sqlStr)
    var endForRead = System.nanoTime()

    println("读取数据耗时："+(endForRead-startForRead)/1000000000+"s")

    df.persist()
    df.registerTempTable("original")

    val allCount = df.rollup(leftField, rightField).count()
    val violations = allCount.groupBy(leftField).count().filter("count > 2").sort(leftField).select(leftField)
    violations.registerTempTable("violation")

    val v = sqlContext.sql("SELECT original." + leftField + "," + rightField + " FROM original,violation WHERE " +
    "original." + leftField + "=violation." + leftField)
    v.persist()  //88888888

    val vio = v.count()
    val tot = df.count()
    println("dirty data's number is " + vio)
    println("total data's number is " + tot)
    println("part of the dirty data are ")
    v.show()

    val stat = Seq(Result(vio, tot, ruleContent))
    val ddf = sqlContext.createDataFrame(stat)

    ddf.registerTempTable("temp")

    ddf.show()

    /*var result = ddf
    if (vio > 0) {
      result = v.join(ddf)
    }
    result.registerTempTable("result")*/

    //val tableName = args(0) + "_" + args(5) + "_" + args(3) + "_" + System.currentTimeMillis
    val newTableName1 = tableName + "_" + ruleType + "_"  + System.currentTimeMillis
    val newTableName=newTableName1.trim()

    println("start writing back result at " + Calendar.getInstance().getTime)

   // sqlContext.sql("CREATE TABLE IF NOT EXISTS sparkdb." + newTableName + " as select * from result")
    val executeSql = "CREATE TABLE IF NOT EXISTS sparkdb." + newTableName + " as select * from temp"
    sqlContext.sql(executeSql)

    println("执行的sql是："+executeSql)

    println("finish detecting and writing back result at " + Calendar.getInstance().getTime)

    var endForProgram = System.nanoTime()
    println("程序总耗时："+(endForProgram-startForProgram)/1000000000+"s")

  }


}
