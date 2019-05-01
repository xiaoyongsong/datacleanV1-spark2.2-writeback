package cn.edu.fudan.tools

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable.Map

object FD1TO1 {

  /**
    *
    *   只修改，没有删除
    *
    *
    */
  def doFDRepair(dataFrame:DataFrame,leftAttr:String,rightAttr:String,context:HiveContext):DataFrame={

    val df = dataFrame.select(leftAttr, rightAttr)

    val df1 = df.rollup(leftAttr, rightAttr).count()
    val df11 = df1.filter(df1(leftAttr).isNotNull)
    df11.persist()

    val df22 = df11.filter(df11(rightAttr).isNotNull)
    val df33 = df11.filter(df11(rightAttr).isNull).withColumnRenamed("count", "count_").
      withColumnRenamed(leftAttr, leftAttr + "_").withColumnRenamed(rightAttr, rightAttr + "_")

    val vio = df22.join(df33, df22(leftAttr) === df33(leftAttr + "_")).filter(df22("count")<df33("count_")&&
      df22("count") > (df33("count_")/2)).select(leftAttr,rightAttr)

    val repairMap:Map[String,String]= Map()

    vio.collect().foreach(row =>{
      repairMap += (row.getString(0) -> row.getString(1))
    } )

    println("修复Map的size:"+repairMap.size)

    def audit(row: Row, cor: Map[String, String]): Row = {
      val left = row.fieldIndex(leftAttr)
      val right = row.fieldIndex(rightAttr)
      val visitedIndex = row.fieldIndex("VISITED")
      var value = row.toSeq
      if (cor.contains(value(left).asInstanceOf[String])){

        value=value.updated(visitedIndex,"1")
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

    val re = context.createDataFrame(dataFrame.rdd.map(row => audit(row, repairMap)), dataFrame.schema)
    re

  }

}
