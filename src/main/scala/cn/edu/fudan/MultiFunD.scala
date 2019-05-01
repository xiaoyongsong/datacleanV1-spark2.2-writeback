package cn.edu.fudan

import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext

//written by QSNorman, 2017 11 13
//实现X1->Y, X2->Y下，Y的等价类建立
//主要方法是建立并查集。利用X2建立X1的并查集，根据并查集的结果建立Y的等价类

object MultiFunD {
	class Result(dirty: Long, total: Long, rule: String)
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setAppName("TwoFD")
		val sc = new SparkContext(conf)
		val sqlContext = new HiveContext(sc)

		//args:表格名称，X1，X2，Y，一个字符串
	//	val args = Array("wonders_dataqualitytest", "zip", "city", "state", "eqclass")
		val df = sqlContext.sql("SELECT * FROM default." + args(0))
		df.persist()

		//将X1->X1建表
		val al = df.rollup(args(1), args(2)).count
		val all = al.filter(al(args(1)).isNotNull)  //标准rollup
		val a1 = all.filter(all(args(2)).isNotNull) //存储所有的X1 X2对应
		val a2 = all.filter(all(args(2)).isNull)  //拎出所有的X1

		var mp0: Map[String, String] = Map()   //mp0一开始为所有的X1->X1
		a2.collect.foreach(row => mp0 += (row.getString(0) -> row.getString(0)))

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

		//一种方法是，产生新表re，对于原df中的X1列进行修改值，内容为mp(X1)
		/* def audit(row: Row): Row = {  //产生修改后的行
			val left = row.fieldIndex(args(1))
			val value = row.toSeq
			val b = value.updated(left, mp0(value(left).asInstanceOf[String]))
			Row.fromSeq(b)
		}
		val re = sqlContext.createDataFrame(df.map(row => audit(row)), df.schema).withColumnRenamed(args(1), args(4)) */

		//另一个方法是，产生新表re，直接在最后添加一列eqclass
		def audit2(row: Row): Row = {
			val left = row.fieldIndex(args(1))
			val value = row.toSeq
			val b = value :+ ufset_find(value(left).asInstanceOf[String])
			Row.fromSeq(b)
		}
		val re = sqlContext.createDataFrame(df.rdd.map(row => audit2(row)), df.schema.add(args(4), "String"))
		re.persist

		re.show()

		/*

		//对上述的新表re进行检测
		val eqshow = re.rollup(args(4), args(3)).count
		val re2 = eqshow.groupBy(args(4)).count.filter("count>1").sort(args(4))  //这些就是有bug的，随便挑一个出来
		re2.filter("count<10").filter("count>7").show   //里面挑一个，比如eqclass=56334。
		re.filter(args(4)+"=56334").show(40,false)    //在re中查询，发现这些行的state的确应该为等价类


*/

		//检测为等价类后，可以根据别的方法对Y进行修改。

	}
}
