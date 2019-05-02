package cn.edu.fudan.TestForDD
import java.util.regex.Pattern

import cn.edu.fudan.Result
import cn.edu.fudan.tools.Rdd2DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import scala.math._
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object TriangleDD {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("TriangleDD")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

  /*  val tableName = args(0)
    val ruleType = args(1)
    val ruleContent = args(2)*/

    val rawTableName = args(0)
    val ruleType = args(1)
    val ruleContent = args(2).replaceAll("\\s+","")
    val hdfspath = args(3)
    val schemaString = args(4)
    val tableId = args(5)
    val userId = args(6)

    var tableName = userId+"_"+tableId+"_"+rawTableName
    tableName = tableName.replaceAll("[^0-9a-zA-Z]","_")

    //val df = sqlContext.sql("select * from "+tableName)
    //val df = sqlContext.sql("select * from test1w_v2")
    val df = Rdd2DataFrame.RddTODF(schemaString,hdfspath,sc,sqlContext)

    println("转化得的DF：")
    df.show()

    val row = df.first()

    val tuple = fun1(ruleContent,row)
    val mapleft = tuple._1
    val mapright = tuple._2

    val total = df.count()
    // val dff = df.takeAsList(total)

    val n=50
    val numofreduce=n*(n+1)/2
    //val sp1=sc.parallelize(sp, n)
   // val sp1=sc.parallelize(df.collect(), n)

    def judge(t1 : Row, t2 : Row) : Int = {
      for (mp <- mapleft)
      {
        val a=mp("attr").toInt
        val c=mp("val").toDouble

        val xa =  t1.getString(a).toDouble
        val ya = t2.getString(a).toDouble

        if (mp("op")=="=" && abs(xa-ya)!=c) return 0
        if (mp("op")=="<" && abs(xa-ya)>=c) return 0
        if (mp("op")=="<=" && abs(xa-ya)>c) return 0
        if (mp("op")==">" && abs(xa-ya)<=c) return 0
        if (mp("op")==">=" && abs(xa-ya)<c) return 0
      }
      for (mp <- mapright)
      {

        val a=mp("attr").toInt
        val c=mp("val").toDouble

        val xa =  t1.getString(a).toDouble
        val ya =  t2.getString(a).toDouble

        if (mp("op")=="=" && abs(xa-ya)!=c) return 1
        if (mp("op")=="<" && abs(xa-ya)>=c) return 1
        if (mp("op")=="<=" && abs(xa-ya)>c) return 1
        if (mp("op")==">" && abs(xa-ya)<=c) return 1
        if (mp("op")==">=" && abs(xa-ya)<c) return 1
      }
      return 0
    }

    //xys   花时间不少  100w 17min
    val reran=df.rdd.flatMap(x => {
      val k=(new util.Random).nextInt(n)+1   //这句能改造，不能new这么多对象
      for (i <- 1 to n) yield (if (i<=k) (k-1)*k/2+i-1 else (i-1)*i/2+k-1, (if (i>k) 1 else if (i==k) 0 else -1, x))
    })

    val tri=reran.partitionBy(new HashPartitioner(numofreduce))
    def mf(iter: Iterator[(Int, (Int, Row))]) : Iterator[Long] = {
      var num : Long = 0
      /*var l=List[List[Double]]()
      var m=List[List[Double]]()
      var r=List[List[Double]]()*/
      var l=List[Row]()
      var m=List[Row]()
      var r=List[Row]()
      var p=0
      /*do {
        var pre=iter.next
        if (pre._2._1==1) l=pre._2._2::l
        else if (pre._2._1==0) m=pre._2._2::m
        else r=pre._2._2::r
      } while (iter.hasNext)*/
      while (iter.hasNext){
        var pre=iter.next
        if (pre._2._1==1) l=pre._2._2::l
        else if (pre._2._1==0) m=pre._2._2::m
        else r=pre._2._2::r
      }
      if (m.size!=0)
      {
        val s=m.size
        for (i<-0 until s ; j<- i+1 until s) num = num + judge(m(i), m(j))
      }
      else for (i<-l; j<-r) num = num + judge(i, j)
      val res:List[Long]=List(num)
      res.iterator
    }

    val ans=tri.mapPartitions(partition => mf(partition))
    val number = ans.reduce(_+_)
    //这里的number就是所有的tuple pair中出错的数量
    System.out.println("end at ")
    //System.out.println(new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss:SSS").format(new Date()));

    println("出错: "+ number)

    val dirty = number

    var newdirty = dirty
    var newtotal = total


    if(dirty<(total*0.01*total)) {  //如何脏数据占比太小，则另结果为0.01，避免比重难以表达，甚至消失。
      newdirty=2
      newtotal=100
      println("执行了，0.02")
      if(dirty==0) newdirty=0
    }

    val stat = Seq(Result(newdirty, (newtotal-1)*newtotal, ruleContent))

    val tempDF = sqlContext.createDataFrame(stat)
    tempDF.registerTempTable("result")
    tempDF.show()
    //resultDF.show()

    val newTableName1 = tableName + "_" + ruleType + "_" + System.currentTimeMillis
    val newTableName=newTableName1.trim()

    //resultDF.registerTempTable("result")
    // sqlContext.sql("CREATE TABLE IF NOT EXISTS sparkdb." + newTableName + " as select * from result")

    val writeBackSql = "CREATE TABLE IF NOT EXISTS sparkdb."+newTableName+" as select * from result"
    sqlContext.sql(writeBackSql)
    println("SuCcess")



  }


  def fun1(ruleContent:String,row:Row) ={

    val lrrules = ruleContent.split("@")
    var leftr = lrrules(0).split(",")
    var rightr = lrrules(1).split(",")

    leftr = leftr.map(x=>x.trim())
    rightr = rightr.map(x=>x.trim())

    val leftArray = fun2(leftr,0,row)
    val riggyArray = fun2(rightr,0,row)


    (leftArray,riggyArray)

  }

  def fun2(arrayRule:Array[String],flag:Int,row:Row): ArrayBuffer[Map[String,String]] ={

    var arryBuffer = ArrayBuffer[Map[String,String]]()
    for(rule<-arrayRule){
      var map:Map[String,String] = Map()
      val attr = rule.replaceAll("[0-9<>=]","")
      var opr = rule.replaceAll("[^<>=]","")
      var pat= Pattern.compile("(^[0-9]+)|([0-9]+$)")
      val matchs =pat.matcher(rule)
      matchs.find()
      val number = matchs.group()

      val index = row.fieldIndex(attr).toString
      map += ("attr"->index)
      map += ("val"->number)

      if(flag==1){
        if("<=".equals(opr)) opr=">"
        else if (">=".equals(opr)) opr="<"
        else if (">".equals(opr)) opr = "<="
        else if ("<".equals(opr)) opr = ">="
        else opr = "!="
      }
      map += ("op"->opr)
      arryBuffer+=map
    }
    return arryBuffer

  }

}
