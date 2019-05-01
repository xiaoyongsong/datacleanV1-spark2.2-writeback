package cn.edu.fudan

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by FengSi on 2017/06/28 at 15:57.
  */
class FD_ {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FunctionDependency")
//      .setMaster("spark://spark:7077")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
//    val spark = new SparkSession(sc)

    val df = sqlContext.read.format("jdbc").options(
      Map("url" -> "jdbc:postgresql://10.141.209.111:5432/test",
        "user" -> "root",
//        "password" -> "mima",
        "dbtable" -> "tb_hospital10k")).load()

//    val allCount = df.rollup("zip", "city").count()
    val allCount = df.rollup(args(0), args(1)).count()

//    val violations = allCount.groupBy("zip").count().filter("count > 2")
    val violations = allCount.groupBy(args(0)).count().filter("count > 2").sort(args(0)).show(10)

//    val violationColumns = allCount.join(violations.select("zip").alias("zip_"),
//      violations("zip === zip_"), "leftouter")
//    val violationColumns = allCount.join(violations.select(args(0)).alias(args(0) + "_"),
//      violations(args(0)) ===  violations(args(0) + "_"), "leftouter")
  }
}
