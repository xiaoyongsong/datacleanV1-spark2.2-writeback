package cn.edu.fudan.DenyConstraint

import java.util.regex.Pattern

import cn.edu.fudan.Result
import cn.edu.fudan.tools.Rdd2DataFrame
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.{ArrayBuffer, Map}

/*
   DC问题
   1.  DC的不确定性：DC由单行的DC，多行之间的DC，每个DC的谓词个数也是不确定的
   2.  不带过滤的join生成表会很大，若带过滤，因为DC的不确定性，选哪个谓词过滤？
   3.  对获得的某条DC表达式解析也是一个挑战
 */

/**
  * 此版本是把单条元组预先过滤的条件全放到sql的where条件中去
  *
  */
object DCDetect_V2 {

  def fun1(exps:String):Map[String,String]={

    var map:Map[String,String] = Map()
    if(exps.contains("A.")&&exps.contains("B."))       map+=("tuple_num"->"2")
    else if (exps.contains("A.")||exps.contains("B.")) {
      val newexps = exps.replaceAll("(A.|B.){1}","")         //支持A.salary>1000这种形式
      println("newExp1: "+newexps)
      map+=("singleExp"->newexps)
      map+=("tuple_num"->"1")
      map+=("originalExp"->exps.replaceAll("\\s+",""))
      return map
    }

    else  return null;    //map+=("tuple_num"->"error")    既无A. 也无B. 说明不符合书写规则，返回。

    if(exps.contains("!=")) map+=("type"->"!=")
    else if (exps.contains(">=")) map+=("type"->">=")
    else if (exps.contains("<=")) map+=("type"->"<=")
    else if (exps.contains("=")) map+=("type"->"=")
    else if (exps.contains(">")) map+=("type"->">")
    else if (exps.contains("<")) map+=("type"->"<")
    else map+=("type"->"error")

    var cout=0
    val first = exps.indexOf(".")
    if(first>=0) cout+=1
    val second = exps.indexOf(".",first+1)
    if(second>=0) cout+=1
    val str1 = exps.substring(first+1,exps.indexOf(map("type")))
    val str2 = exps.substring(second+1)
    if(cout==1) {
      map += ("field_num" -> "error")
    }
    else if(cout==2) {
      map += ("field_num" -> "2")
      map += ("field1" -> str1)
      map += ("field2" -> str2)
    }
    else {
      map+=("field_num"->"error")
    }
    return map
  }

  def fun2(arrayMap:ArrayBuffer[Map[String,String]]):(ArrayBuffer[Map[String,String]],ArrayBuffer[Map[String,String]])={

    var map1 = ArrayBuffer[Map[String,String]]()
    var map2 = ArrayBuffer[Map[String,String]]()

    for(map<-arrayMap){
      if("1".equals(map("tuple_num")))
        map1+=map
      if("2".equals(map("tuple_num")))
        map2+=map
    }
    return (map1,map2)

  }

  /**
    * 此版本是把单条元组预先过滤的条件全放到sql的where条件中去
    *
    */
  def main(args: Array[String]): Unit = {

    var startForProgram = System.nanoTime()

    val conf = new SparkConf().setAppName("DenyConstraint_Detect")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    // val args =Array("wonders_dataqualitytest","dc","A.city=B.city@A.zip!=B.zip@A.salary>15000")


    val rawTableName = args(0)
    val ruleType = args(1)
    val ruleContent = args(2).replaceAll("\\s+","")
    val hdfspath = args(3)
    val schemaString = args(4)
    val tableId = args(5)
    val userId = args(6)

    var tableName = userId+"_"+tableId+"_"+rawTableName
    tableName = tableName.replaceAll("[^0-9a-zA-Z]","_")


    println("spark端收到的参数！")
    println("tableName: "+tableName+"\nruleType: "+ruleType+" \nruleContent:"+ruleContent)
    println("hdfspath: "+hdfspath+" \nschemaString: "+schemaString+" \ntableId: "+tableId+"\nuserId: "+userId)

    val strArry:Array[String] = ruleContent.split("@")

    var arrayMap = ArrayBuffer[Map[String,String]]()
    for(str<-strArry){
      arrayMap+=fun1(str)
    }
    val resTuple:(ArrayBuffer[Map[String,String]],ArrayBuffer[Map[String,String]])=fun2(arrayMap)
    val tuples_1 = resTuple._1      //表示一个云元组上的谓词集合
    val tuples_2 = resTuple._2      //表示涉及2个元组的谓词集合，每个谓词是一个map

    var sets:Set[String] = Set()         //搜集涉及的字段，用来简化读取的数据量大小
    for(t1<-tuples_1){

      println("一个元组的表达式： "+t1("singleExp"))
      val fs = t1("singleExp").replaceAll("(>|<|>=|<=|=|!=){1}","@")  //去掉关系符号
      val fields = fs.split("@")
      var pattern = Pattern.compile("\\d+")

      for(ss<-fields){
        var matcher = pattern.matcher(ss)                //检查是不是数字，是数字的话就不用列出来
        if(!matcher.matches())
          sets+=ss
      }
    }
    for(t2<-tuples_2){
      println("两个元组的表达式： "+t2("field1")+t2("type")+t2("field2"))
      sets+=t2("field1")
      sets+=t2("field2")
    }

    var setToArray = sets.toArray
    var originalSelect=""
    for(i <- 0 until setToArray.length){
      if(i!=setToArray.length-1)
        originalSelect+=setToArray(i)+","
      else
        originalSelect+=setToArray(i)+" "
    }



    val rawRDDdf = Rdd2DataFrame.RddTODF(schemaString,hdfspath,sc,sqlContext)

    rawRDDdf.registerTempTable("RddDf")
    val originalSql = "select "+originalSelect+" from RddDF"

    val df = sqlContext.sql(originalSql)


    println("Rdd转化来的DF：")
    df.show()



    /*var startForRead = System.nanoTime()
    val df = sqlContext.sql(originalSql)
    var endForRead = System.nanoTime()
    println("读取数据耗时："+(endForRead-startForRead)/1000000000+"s")*/

    //println("读取数据库的sql语句是："+originalSql)

    var temp=df
    val total = df.count()

    println("原始total："+total)

    var colomnName = ArrayBuffer[String]()             //搜集写回结果时只涉及到的字段名

    var singleWhere=""

    for(t1<-tuples_1){                  //先用只涉及单个元组的谓词过滤，简化表的大小

      println("singleExp: "+t1("singleExp"))
      val fs = t1("singleExp").replaceAll("(>|<|>=|<=|=|!=){1}","@")  //去掉关系符号
      val sign = t1("singleExp").replaceAll("[A-Za-z0-9\\.]","")     //得到连接符

      val fields = fs.split("@")
      var pattern = Pattern.compile("\\d+")

      for(ss<-fields){
        var matcher = pattern.matcher(ss)                //检查是不是数字，是数字的话就不用列出来
        if(!matcher.matches()){
          colomnName+="A."+ss+" as "+ "A_"+ss
        }
      }
      val newExp = fields(0)+sign+fields(1)    //拼接成合法表达式
      println("过滤语句newExp: "+newExp)

      /**
        * 这块先不用了，全写在where语句里面，为了处理A.salary>10000@B.salary<8000这种情况
        */
      //temp=temp.where(newExp)              //执行了- -！！  当然能保证能执行，因为能进入for循环说明有单元组的过滤

      singleWhere+=t1("originalExp")+" AND "

    }

    temp.persist()

    val copyDf = temp.toDF()
    temp.registerTempTable("A")
    copyDf.registerTempTable("B")

    var sqlStr =""                //构建涉及两个元组的where中的表达式，sql
    var ct=0
    for(s<-tuples_2){
      ct+=1
      if(tuples_2.length!=ct){
        sqlStr+="A."+s("field1")+s("type")+"B."+s("field2")+" AND "
      }
      else{
        sqlStr+="A."+s("field1")+s("type")+"B."+s("field2")
      }
      colomnName+="A."+s("field1")+" as "+"A_"+s("field1")
      colomnName+="B."+s("field2")+" as "+"B_"+s("field2")
    }

    var selectStr = ""                  //构建写回数据库时，要写那些字段
    for(i<- 0 until colomnName.length ){
      if(i!=colomnName.length-1)
        selectStr+=colomnName(i)+","
      else selectStr+=colomnName(i)+" "
    }

    var dirty=0L
    var executeSql=""
    if(sqlStr.length>0){            //说明有涉及两元组的过滤条件
      // var newDF1 = sqlContext.sql("select "+selectStr+" from A,B where "+sqlStr)

      executeSql = "select count(*) from A,B where "+singleWhere+sqlStr
      println("执行的sql语句是： "+executeSql)
      val newDF1 = sqlContext.sql(executeSql)
      //dirty = Integer.parseInt(newDF1.first()(0).toString)
      dirty = newDF1.first().getLong(0)


      /*newDF1.registerTempTable("newDF")
      println("冲突数："+newDF1.count())
      newDF1.show()*/
    }
    else {
      executeSql = "select count(*) from A where "+singleWhere
      println("执行的sql语句是： "+executeSql)
      val newDF2 = sqlContext.sql(executeSql)
      //dirty = Integer.parseInt(newDF2.first()(0).toString)
      dirty = newDF2.first().getLong(0)



      /*println("冲突数："+newDF2.count())
      newDF2.registerTempTable("newDF")
      newDF2.show()*/
    }


    var newdirty = dirty
    var newtotal = total

    println("---------------dirty--------"+dirty+"-----------total-- ------"+total)



    if(dirty<(total*0.01*total)) {  //如何脏数据占比太小，则另结果为0.01，避免比重难以表达，甚至消失。
        newdirty=2
        newtotal=100
      println("执行了，0.01")
      if(dirty==0) newdirty=0
    }

    println("---------------newdirty--------"+newdirty+"-----------newtotal-- ------"+newtotal)


    val stat = Seq(Result(newdirty, (newtotal-1)*newtotal, ruleContent))
    val tempDF = sqlContext.createDataFrame(stat)
    tempDF.registerTempTable("result")


    tempDF.show()
    // newDF.registerTempTable("newDF")
    val newTableName1 = tableName + "_" + ruleType + "_" + System.currentTimeMillis
    val newTableName=newTableName1.trim()
    println("newTableName:"+newTableName)

    //println("CREATE TABLE IF NOT EXISTS sparkdb."+newTableName+" as select * from newDF")
    //sqlContext.sql("CREATE TABLE IF NOT EXISTS sparkdb." + newTableName + " as select * from newDF")



    val writeBackSql = "CREATE TABLE IF NOT EXISTS sparkdb." + newTableName + " as select * from result"
    println("写回的sql是："+writeBackSql)
    sqlContext.sql(writeBackSql)
    val endForProgram = System.nanoTime()
    println("程序总耗时："+(endForProgram-startForProgram)/1000000000+"s")

    println("SuCcess")

  }

}
