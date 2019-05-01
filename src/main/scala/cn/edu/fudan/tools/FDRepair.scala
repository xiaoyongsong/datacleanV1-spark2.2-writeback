package cn.edu.fudan.tools

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable.ArrayBuffer

object FDRepair {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("test")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    val df = sqlContext.sql("select * from temp")

    val aa:List[ArrayBuffer[String]]=List(ArrayBuffer("a"),ArrayBuffer("a","b"),ArrayBuffer("b","c"),ArrayBuffer("a","b","c"))

  }

  /**
    *
    *    返回修复后的结果，drop掉repair过程中产生的临时列
    */
  def doMFDRepair(dataFrame: DataFrame,lefts:List[ArrayBuffer[String]], right:String,context:HiveContext):DataFrame={
    val ll = lefts(0)
    var exeDF = dataFrame
    if(ll.length==1){
     // return FD1TO1.doFDRepair(exeDF,ll(0),right,context)
      return FD1TO1_V2.doFDRepair(exeDF,ll(0),right,context)
    }

    def addCol(row: Row, indexs: ArrayBuffer[Int]): Row = {
      var newRow = row.toSeq
      var newVal = ""
      for (i <- indexs) {
        newVal += newRow(i).toString + "_"
      }
      newRow = newRow :+ newVal
      Row.fromSeq(newRow)
    }

    val tempRow = exeDF.first()
    var index:ArrayBuffer[Int]=ArrayBuffer[Int]()
    var addName = ""
      for (t <- ll) {
        index += tempRow.fieldIndex(t)
        addName += t + "_"
      }
    exeDF = context.createDataFrame(exeDF.rdd.map(x => addCol(x, index)), exeDF.schema.add(addName, "string"))
    val resDF = FD1TO1_V2.doFDRepair(exeDF,addName,right,context)

    resDF.drop(addName)

  }


  /**
    * 传入一个dataFrame,一条涉及等价类的FD，还有一个HiveContext，
    *
    * 返回修复后的结果，drop掉repair过程中产生的临时列
    *
    */

  def doEqualClassRepair(dataFrame: DataFrame,lefts:List[ArrayBuffer[String]], right:String,context:HiveContext):DataFrame={

    val datatFrameANDDropCol = getNewDF(dataFrame,lefts,context)
    val exeDF = datatFrameANDDropCol._1                    //得到增加了组合列的DF
    var dropColName = datatFrameANDDropCol._2

    val eqExp1 = lefts(0)
    val eqExp2 = lefts(1)
    var name1 = getCombineName(eqExp1)
    var name2 = getCombineName(eqExp2)

    var tuple = EqualClass.eqclass(exeDF,name1,name2,right,context)
    dropColName+=tuple._2

    for(i <- 2 until(lefts.length)){
      val temp = getCombineName(lefts(i))
      tuple = EqualClass.eqclass(tuple._1,tuple._2,temp,right,context)
      dropColName += tuple._2
    }

    var resultDF = tuple._1

    println("建立完等价类后的结果：")
    resultDF.show()

    var resDF = FD1TO1_V2.doFDRepair(resultDF,tuple._2,right,context)

    println("按等价类 "+tuple._2+" 修复的结果")
    resDF.show()

    for( name <- dropColName){
      resDF = resDF.drop(name)
    }
    resDF

  }

  def getCombineName(str:ArrayBuffer[String]):String={
    var name=""
    if(str.length==1) return  str(0)
    else {
      for(t <- str){
        name+=t+"_"
      }
    }
    name
  }

  /**
    *  传入dataframe，拼接col，增加col，并返回增加组合列的DF和新增的Column的数组，用于还原df
    *
    *  如 表 scheme  [ a,b,c]  => [a, b, c, a_b_, a_c_, a_b_c_]     dropArray(a_b_, a_c_, a_b_c_)
    */

  def getNewDF(dataFram:DataFrame,lefts:List[ArrayBuffer[String]],context:HiveContext):(DataFrame,ArrayBuffer[String])= {

    var addColArr = lefts.filter(x => x.length > 1)

    var exeDF = dataFram
    def addCol(row: Row, indexs: ArrayBuffer[Int]): Row = {
      var newRow = row.toSeq
      var newVal = ""
      for (i <- indexs) {
        newVal += newRow(i).toString + "_"
      }
      newRow = newRow :+ newVal
      Row.fromSeq(newRow)
    }

    val dropName:ArrayBuffer[String]=ArrayBuffer[String]()
    for (name <- addColArr) {
      val tempRow = exeDF.first()
      val index: ArrayBuffer[Int] = ArrayBuffer[Int]()
      var addName = ""
      for (t <- name) {
        index += tempRow.fieldIndex(t)
        addName += t + "_"
      }
      dropName+=addName
      exeDF = context.createDataFrame(exeDF.rdd.map(x => addCol(x, index)), exeDF.schema.add(addName, "string"))
    }
    (exeDF,dropName)
  }




}
