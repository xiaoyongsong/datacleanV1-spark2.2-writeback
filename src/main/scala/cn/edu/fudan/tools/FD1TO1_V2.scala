package cn.edu.fudan.tools

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable.Map
import scala.collection.mutable

object FD1TO1_V2 {

  /**
    *
    *   只修改，没有删除
    *
    *   这个版本更改了修改的依据 ， 不是按照过半的项为正确项， 按出现最多的项作为正确项
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

   /* val vio = df22.join(df33, df22(leftAttr) === df33(leftAttr + "_")).filter(df22("count")<df33("count_")&&
      df22("count") > (df33("count_")/2)).select(leftAttr,rightAttr)*/

    val vv = df11.groupBy(df11(leftAttr)).count().filter("count>2").select(leftAttr).
      withColumnRenamed(leftAttr,leftAttr+"_")

    val vvv = vv.join(df22,vv(leftAttr+"_") === df22(leftAttr)).select(leftAttr,rightAttr,"count")

   //  val vio = df22.join(df33, df22(leftAttr) === df33(leftAttr + "_"))

    val repairMap:mutable.Map[String,(String,Long)]= mutable.Map()

/*    vio.collect().foreach(row =>{
      repairMap += (row.getString(0) -> row.getString(1))
    } )*/

    vvv.persist()

    vvv.collect().foreach(row=>{

      val lef = row.getString(0)
      val rig = row.getString(1)
      val count = row.getLong(2)

      if(repairMap.contains(lef)){              //这里是做选择出现次数最多的作为要修改的结果
        val tt = repairMap(lef)
        if(count>tt._2) {

          repairMap(lef)=(rig,count)
        }
        if(count==tt._2){
          if(rig<tt._1)
            repairMap(lef)=(rig,count)
          else
            repairMap(lef)=(tt._1,count)
        }


      }
      else repairMap(lef) = (rig,count)
    })

    println("修复Map的size:"+repairMap.size)

    def audit(row: Row, cor: mutable.Map[String, (String,Long)]): Row = {
      val left = row.fieldIndex(leftAttr)
      val right = row.fieldIndex(rightAttr)
      val visitedIndex = row.fieldIndex("VISITED")
      var value = row.toSeq
      if (cor.contains(value(left).asInstanceOf[String])){

        value=value.updated(visitedIndex,"1")
        if(!value(right).asInstanceOf[String].equals(cor(value(left).asInstanceOf[String])._1)){
          val b = value.updated(right, cor(value(left).asInstanceOf[String])._1)
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
