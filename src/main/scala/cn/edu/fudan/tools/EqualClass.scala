package cn.edu.fudan.tools

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row}


object EqualClass {

  /**
    *   传入df,在其上面建立个等价类，返回这新的df和等价类的名字
    *
    *   如 传入的df schema [a,b,c, a_b_, b_c_ ]    若是建立 a 和 b_c_的在right上的等价类 则返回
    *   [a,b,c, a_b_, b_c, a#b_c_#right] 和 a#b_c_#right属性
    */
  def eqclass(dataFrame: DataFrame,left1: String,left2:String,right:String,context:HiveContext):(DataFrame,String)={

    val df = dataFrame
    val al = df.rollup(left1, left2).count
    val all = al.filter(al(left1).isNotNull)  //标准rollup
    val a1 = all.filter(all(left2).isNotNull) //存储所有的X1 X2对应
    val a2 = all.filter(all(left2).isNull)  //拎出所有的X1

    var mp0: Map[String, String] = Map()   //mp0一开始为所有的X1->X1
    a2.collect.foreach(row => mp0 += (row.getString(0) -> row.getString(0)))

    var mp1: Map[String, String] = Map()   //mp1一开始为空

    def ufset_find(str: String) : String = {  //并查集的查找
      if (str!=mp0(str)) mp0 += (str -> ufset_find(mp0(str)))
      mp0(str)
    }

    def ufset_union(str0: String, str1: String) = {  //并查集的合并
      val p = ufset_find(str0)
      val q = ufset_find(str1)
      if (p!=q) mp0 += (p -> q)
    }

    def uf(row: Row) = {  //不返回值，就只修改表
      val left = row.fieldIndex(left1)
      val right = row.fieldIndex(left2)
      val value = row.toSeq
      val lv = value(left).asInstanceOf[String]
      val rv = value(right).asInstanceOf[String]

      if (!mp1.contains(rv)) mp1 += (rv -> lv) //如果X2是第一次查到
      else ufset_union(lv, mp1(rv))
    }

    a1.collect.foreach(row => uf(row))   //用并查集进行修改

    //另一个方法是，产生新表re，直接在最后添加一列eqclass
    def audit2(row: Row): Row = {
      val left = row.fieldIndex(left1)
      val value = row.toSeq
      val b = value :+ ufset_find(value(left).asInstanceOf[String])
      Row.fromSeq(b)
    }

    val eqClassName=left1+"#"+left2+"->"+right
    val re = context.createDataFrame(df.rdd.map(row => audit2(row)), df.schema.add(eqClassName, "string"))

    (re,eqClassName)

  }

}
