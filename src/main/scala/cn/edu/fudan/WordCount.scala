package cn.edu.fudan

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by FengSi on 2017/06/07 at 15:28.
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("spark://10.141.220.81:7077")
//        .setJars(Array("D:\\Documents\\IdeaProjects\\dataclean\\out\\artifacts\\dataclean_jar"))
//      .setIfMissing("spark.driver.host", "10.131.243.171")
    val sc = new SparkContext(conf)
    val text = sc.textFile("hdfs://10.141.220.81:9000/user/hadoop/share/lib/sharelib.properties").
      flatMap(line => line.split(" ")).
      map(word => (word, 1)).
      reduceByKey(_+_)

    val a = text.count()
    println("总共有行数： " + a)

  }
}
