package cn.edu.fudan

import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext

//written by QSNorman, 2017 11 7
//实现X1->Y, X2->Y下，Y的等价类建立
//主要方法是建立并查集。利用X2建立X1的并查集，根据并查集的结果建立Y的等价类

object TWOFD {
  class Result(dirty: Long, total: Long, rule: String)
  def main(args: Array[String]): Unit = {
  val conf = new SparkConf().setAppName("TwoFD")
  val sc = new SparkContext(conf)
  val sqlContext = new HiveContext(sc)

  //args:表格名称，X1，X2，Y，一个字符串
  //val args = Array("wonders_dataqualitytest", "zip", "city", "state", "eqclass")


    /**
      * xys_只select涉及属性
      */
    val tableName = args(0)
    val ruleType = args(1)
    val ruleContent = args(2).replaceAll("\\s+","")

    var originalSelect =""
    val attributesLeft = ruleContent.split("@")(0).split(",")
    val attributesRight = ruleContent.split("@")(1)
    for(i <- 0 until attributesLeft.length){
      originalSelect+=attributesLeft(i)+","
    }
    for(i <- 0 until attributesRight.length){
      originalSelect+=attributesRight(i)
    }


  val df = sqlContext.sql("SELECT "+originalSelect+" FROM default." + args(0))
  df.persist()


  //将X1->X1建表
  val al = df.rollup(args(1), args(2)).count
  val all = al.filter(al(args(1)).isNotNull)  //标准rollup
  val a1 = all.filter(all(args(2)).isNotNull) //存储所有的X1 X2对应
  val a2 = all.filter(all(args(2)).isNull)  //拎出所有的X1

  var mp0: Map[String, String] = Map()   //mp0一开始为所有的X1->X1
  a2.foreach(row => mp0 += (row.getString(0) -> row.getString(0)))

  var mp1: Map[String, String] = Map()   //mp1一开始为空

  def ufset_find(str: String) : String = {  //并查集的查找
  if (str!=mp0(str)) mp0 += (str -> ufset_find(mp0(str)))
  mp0(str)
}

  def ufset_union(str0: String, str1: String) = {  //并查集的合并
  val p = ufset_find(str0);
  val q = ufset_find(str1);
  if (p!=q) mp0 += (p -> q);
}

  def uf(row: Row) = {  //不返回值，就只修改表
  val left = row.fieldIndex(args(1))
  val right = row.fieldIndex(args(2))
  val value = row.toSeq
  val lv = value(left).asInstanceOf[String]
  val rv = value(right).asInstanceOf[String]

  if (!mp1.contains(rv)) mp1 += (rv -> lv) //如果X2是第一次查到
  else ufset_union(lv, mp1(rv))
}

  a1.collect.foreach(row => uf(row))   //用并查集进行修改


  //另一个方法是，产生新表re，直接在最后添加一列eqclass
  def audit2(row: Row): Row = {
  val left = row.fieldIndex(args(1))
  val value = row.toSeq
  val b = value :+ ufset_find(value(left).asInstanceOf[String])
  Row.fromSeq(b)
}
  val rep = sqlContext.createDataFrame(df.rdd.map(row => audit2(row)), df.schema.add(args(4), "String"))
  rep.persist

  //检测为等价类后，可以根据别的方法对Y进行修改。

  val arg2 = Array(args(0), args(4), args(3), "repair", "repair")
  val asl = rep.rollup(arg2(1), arg2(2)).count
  val asll = asl.filter(asl(arg2(1)).isNotNull)
  val as1 = asll.filter(asll(arg2(2)).isNotNull)
  val as2 = asll.filter(asll(arg2(2)).isNull).withColumnRenamed(arg2(1), arg2(1) + "_").
  withColumnRenamed(arg2(2), arg2(2) + "_").withColumnRenamed("count", "count_")
  val repair = as1.join(as2, as1(arg2(1)) === as2(arg2(1) + "_")).
  filter(as1("count") < as2("count_") && as1("count") > as2("count_")/2).select(arg2(1), arg2(2))

  var cor: Map[String,String] = Map()
  repair.collect.foreach(row => cor += (row.getString(0) -> row.getString(1)))

  def audit(row: Row, cor: Map[String, String]): Row = {
  val left = row.fieldIndex(arg2(1))
  val right = row.fieldIndex(arg2(2))
  val value = row.toSeq
  if (cor.contains(value(left).asInstanceOf[String]) &&
  !value(right).asInstanceOf[String].equals(cor(value(left).asInstanceOf[String]))) {
  val b = value.updated(right, cor(value(left).asInstanceOf[String]))
  Row.fromSeq(b)
} else {
  row
}
}
  val re = sqlContext.createDataFrame(rep.rdd.map(row => audit(row, cor)), rep.schema)
  re.count
  //    val re = rep.map(row => audit(row, cor))
  // test


  val violations = rep.rollup(arg2(1), arg2(2)).count.groupBy(arg2(1)).count.
  filter("count > 2").sort(arg2(1)).select(arg2(1)).withColumnRenamed(arg2(1), arg2(1) + "_")

  val v = rep.join(violations, rep(arg2(1)) === violations(arg2(1) + "_")).drop(arg2(1) + "_")
  v.count

  println("dirty data's number is " + v.count())
  println("total data's number is " + rep.count())
  println("part of the dirty data are ")


  val violationss = re.rollup(arg2(1), arg2(2)).count.groupBy(arg2(1)).count.
  filter("count > 2").sort(arg2(1)).select(arg2(1)).withColumnRenamed(arg2(1), arg2(1) + "_")

  val vv = re.join(violationss, re(arg2(1)) === violationss(arg2(1) + "_")).drop(arg2(1) + "_")

  println("dirty data's number is " + vv.count())
  println("total data's number is " + re.count())

  val stat = Seq(Result(v.count(), rep.count(), arg2(4)))
  val drep = sqlContext.createDataFrame(stat)

  val result = v.join(drep)
  result.registerTempTable("result")

  val tableName1 = arg2(0) + "_" + arg2(3) + "_" + System.currentTimeMillis

  println("start writing back result at " + Calendar.getInstance().getTime)

  sqlContext.sql("CREATE TABLE IF NOT EXISTS sparkdb." + tableName1 + " as select * from result")

  println("finish detecting and writing back result at " + Calendar.getInstance().getTime)
}
}
