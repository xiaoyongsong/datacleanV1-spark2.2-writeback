package cn.edu.fudan.MultiFD

import cn.edu.fudan.Result
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ArrayBuffer

object MFD {

  def main(args: Array[String]): Unit = {

    var startForProgram = System.nanoTime()
   // val args = Array("wonders_dataqualitytest", "MFD", "zip,city@fname")

    val tableName = args(0)
    val ruleType = args(1)
    val ruleContent = args(2).replaceAll("\\s+","")

    val conf = new SparkConf().setAppName("MFD_Detect")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    var originalSelect =""
    val attributesLeft = ruleContent.split("@")(0).split(",")
    val attributesRight = ruleContent.split("@")(1)
    for(i <- 0 until attributesLeft.length){
        originalSelect+=attributesLeft(i)+","
    }
    for(i <- 0 until attributesRight.length){
      originalSelect+=attributesRight(i)
    }

    println("名字：" + tableName + " 规则：" + ruleType + " 内容: " + ruleContent)
   // val sqlStr = "SELECT * FROM default." + tableName
    val sqlStr = "SELECT "+originalSelect+" FROM default." + tableName
    println("读取数据库的sql语句是："+sqlStr)

    var startForRead = System.nanoTime()
    val df = sqlContext.sql(sqlStr)
    var endForRead = System.nanoTime()
    println("读取数据耗时："+(endForRead-startForRead)/1000000000+"s")

    val total = df.count()
    val fields = ruleContent.split("[,@]{1}")

    var sqlWhere = ""
    var selectedStr = ""
    for (i<- 0 until fields.length) {
      if (i != fields.length - 1){
        sqlWhere += "A." + fields(i) + "=" + "B." + fields(i) + " AND "
        selectedStr+="A."+fields(i)+" as "+"A_"+fields(i)+","+"B."+fields(i)+" as "+"B_"+fields(i)+","
      }
      else{
        sqlWhere += "A." + fields(i) + "!=" + "B." + fields(i)
        selectedStr+="A."+fields(i)+" as "+"A_"+fields(i)+","+"B."+fields(i)+" as "+"B_"+fields(i)
      }
    }
    println("sqlwhere: " + sqlWhere)
    println("selectdStr: "+selectedStr)


    val dfA = df.toDF()
    dfA.registerTempTable("A")
    df.registerTempTable("B")

    println("执行的sql语句是: "+"select "+selectedStr+" from A,B where "+sqlWhere)

   // val resultDF = sqlContext.sql("select "+selectedStr+" from A,B where "+sqlWhere)

    val resultDF = sqlContext.sql("select count(*) from A,B where "+sqlWhere)

    val newTableName1 = tableName + "_" + ruleType + "_" + System.currentTimeMillis
    val newTableName=newTableName1.trim()

   // resultDF.registerTempTable("result")
   // sqlContext.sql("CREATE TABLE IF NOT EXISTS sparkdb." + newTableName + " as select * from result")

    val dirty = resultDF.first().getLong(0)


   /* val creatTableSql = "CREATE TABLE IF NOT EXISTS sparkdb."+newTableName+"(dirty string,total string," +
      "rule string)"
    val inserSql = "INSERT INTO sparkdb."+newTableName+" VALUES("+dirty+","+total+","+ruleType+")"*/

  /*  val row = Row(dirty,total,ruleType)
    val list:List[Row]= List(row)*/

   /* println("创建表："+creatTableSql)
    println("插入表: "+inserSql)*/

 /*   val temp1 = sqlContext.sql(creatTableSql)
    val structType = temp1.schema*/

    val stat = Seq(Result(dirty/2, total*(total-1)/2, ruleContent))
    val tempDF = sqlContext.createDataFrame(stat)
    tempDF.registerTempTable("temp")

    tempDF.show()
    sqlContext.sql("CREATE TABLE IF NOT EXISTS sparkdb." + newTableName + " as select * from temp")

    println("CREATE TABLE IF NOT EXISTS sparkdb." + newTableName + " as select * from temp")

    var endForProgram = System.nanoTime()
    println("程序总耗时："+(endForProgram-startForProgram)/1000000000+"s")

  }

}

