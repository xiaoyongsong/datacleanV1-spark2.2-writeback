package cn.edu.fudan.tools

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object Rdd2DataFrame {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    val path = args(0)
    val schemaString = args(1)

    //val schemaString = "adID,appPlatform,camgaignID,appID,creativeID,advertiserID"
    // val path = "hdfs://10.190.88.200:8020/wonders/root/6aa13792-c7a0-4e90-9db5-abba1603e360"
    // val path = "hdfs://10.141.209.109:9000/input/ttt"
    println("RDD中收到的hdfs路径："+path)
    println("表头："+schemaString)

    val df =RddTODF(schemaString,path,sc,sqlContext)
      df.show()
      println(df.count())
    //df.write.text("hdfs://10.141.209.109:9000/input/tttxys")
   // df.write.save("hdfs://10.141.209.109:9000/xys")
    //df.save("hdfs://10.141.209.109:9000/xys/tt")
   // val schema = StructType(schemaString.split(",").map(fieldName => StructField(fieldName.trim, StringType, true)))
   // println(schema.toString())
  }

  def RddTODF(schemaString:String,path:String,sc:SparkContext,context:HiveContext):DataFrame={

   val schema = StructType(schemaString.split(",").map(fieldName => StructField(fieldName.trim, StringType, true)))

    val rdd = sc.textFile(path).map(_.split(",")).map(tuple=>{
      val seq = tuple.toSeq
      Row.fromSeq(seq)
    })
    context.createDataFrame(rdd,schema)

  }

}
