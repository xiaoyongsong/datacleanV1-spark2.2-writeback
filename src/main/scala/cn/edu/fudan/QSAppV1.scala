package cn.edu.fudan

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, Map}

object QSAppV1 {

  def main(args: Array[String]): Unit = {

    /*
    val name = "hdfs://10.141.209.109:9000/user/root/0106Tax.csv"
    val sizeofhash = 1000000
    var numpar = 30
    val nameadd = "hdfs://10.141.209.109:9000/user/root/0106Tax-add.csv"
    val numparadd = 3
    */

    val name = "hdfs://10.141.209.109:9000/user/root/0103data_10w_4A.csv"
    val sizeofhash = 1000000
    var numpar = 40
    val nameadd = "hdfs://10.141.209.109:9000/user/root/0103add_10w+1000_4A.csv"
    val numparadd = 3

    //xysADD
    val conf = new SparkConf().setAppName("QScala")
    val sc = new SparkContext(conf)


    def makechain(tf:org.apache.spark.rdd.RDD[List[Int]], sizeofhash:Int, no_a:Int, no_c:Int, numpar:Int, k:Double, delta:Double): org.apache.spark.rdd.RDD[(Int, Array[(Int, Int, List[Int])])] = {
      val rddac1=tf.map(x=>List[Int](x(0), x(no_a), x(no_c)))
      val rddac2=rddac1.map(x=>(((k*x(1)+x(2))/delta).toInt,x))
      val rddac3=rddac2.combineByKey(List(_), (x:List[List[Int]], y:List[Int]) => y::x, (x:List[List[Int]], y:List[List[Int]]) => x:::y)
      val rddac4=rddac3.flatMap(x=>{
        val y=x._2.sortBy(a=>(k*a(1)+a(2),a(1))).map(a=>List[Int](a(1), a(2), a(0)))
        val len=y.length.toInt
        val key=x._1
        val arr=new ArrayBuffer[(Int, Int, List[Int])]
        var tmp = (y(0)(0), y(0)(1), List[Int](y(0)(2)))
        val ans = new ArrayBuffer[(List[Int], (Int, Int, List[Int]))]
        ans+=((List[Int](key, -1, 0), tmp))
        ans+=((List[Int](key, -1, 0), tmp))
        ans+=((List[Int](key, 0, 0), tmp))
        var arrsz=0
        for (i <- 1 to len) {
          if (i!=len && y(i)(0)==tmp._1 && y(i)(1)==tmp._2) {
            tmp=(tmp._1, tmp._2, y(i)(2)::tmp._3)
          }
          else {
            var dis:Double = 10000
            var idx = -1
            var a=tmp._1
            var c=tmp._2
            for (j<- 0 to arrsz-1) {
              if (a>arr(j)._1 && c>arr(j)._2 && dis>(k*(a-arr(j)._1)+c-arr(j)._2) ) {
                dis = k*(a-arr(j)._1)+c-arr(j)._2.toDouble
                idx = j
              }
            }
            if (idx == -1) {
              arr+=tmp
              idx=arrsz
              arrsz=arrsz+1
              ans+=((List[Int](key, -1, idx), tmp))
              ans+=((List[Int](key, 0, idx), tmp))
            }
            else {
              ans+=((List[Int](key, 0, idx), tmp))
              arr(idx)=tmp
            }
            if (i<len) {
              tmp=(y(i)(0), y(i)(1), List[Int](y(i)(2)))
            }
          }
        }
        for (j <- 0 to arrsz-1) ans += ((List[Int](key, 1, j), arr(j)))
        ans.toList
      })
      val rddac5 = rddac4.filter(_._1(1)!=0).map(x=>(if(x._1(1) == -1) x._1(0)-1 else x._1(0), (if(x._1(1) == -1) 1 else 0, x._1(2), x._2._1, x._2._2))).filter(_._1 >= 0)
      val rddac6 = rddac5.combineByKey(List(_), (x:List[(Int, Int, Int, Int)], y:(Int, Int, Int, Int)) => y::x, (x:List[(Int, Int, Int, Int)], y:List[(Int, Int, Int, Int)]) => x:::y).flatMap(x=>{
        var head = new ArrayBuffer[(Int, Int, Int)]
        var tail = new ArrayBuffer[(Int, Int, Int)]
        for (ele <- x._2) {
          if (ele._1 == 0) tail += ((ele._3, ele._4, ele._2))
          else head += ((ele._3, ele._4, ele._2))
        }
        head = head.sortBy(x=> (x._2-k*x._1, -x._1))
        tail = tail.sortBy(x=> (x._2-k*x._1, -x._1))
        val hs = head.size
        val ts = tail.size
        val ans = new ArrayBuffer[((Int, Int), (Int, Int))]
        var i=0
        var j=0
        while (i<hs && j<ts){
          if (head(i)._1>tail(j)._1 && head(i)._2>tail(j)._2) {
            ans += (((x._1, tail(j)._3), (x._1+1, head(i)._3)))
            i=i+1
            j=j+1
          }
          else {
            if (head(i)._1<=tail(j)._1) j=j+1
            else i=i+1
          }
        }
        ans
      })
      var arrac=rddac6.collect.sortBy(_._1)
      val mapac = Map[Int, Int]()
      var curnum=0
      var curhash1=0
      var curhash2=0
      arrac.foreach(x=>{
        curhash1=x._1._1*sizeofhash+x._1._2
        curhash2=x._2._1*sizeofhash+x._2._2
        if (mapac.contains(curhash1)) {
          mapac += (curhash2 -> mapac(curhash1))
        }
        else {
          mapac += (curhash1 -> curnum)
          mapac += (curhash2 -> curnum)
          curnum = curnum+1
        }
      })
      val rddac7 = rddac4.filter(_._1(1)==0).map(x=>((x._1(0), x._1(2)), x._2))
      val transac = mapac.toList.map(x=>((x._1/sizeofhash, x._1%sizeofhash), x._2))
      val trparac = sc.parallelize(transac, numpar)
      val joinac1 = rddac7.leftOuterJoin(trparac)
      val joinac2 = joinac1.map(x=>(x._2._2.getOrElse((1+x._1._1)*sizeofhash+x._1._2), x._2._1))
      val joinac3 = joinac2.combineByKey(ArrayBuffer(_), (x:ArrayBuffer[(Int, Int, List[Int])], y:(Int, Int, List[Int])) => x+=y, (x:ArrayBuffer[(Int, Int, List[Int])], y:ArrayBuffer[(Int, Int, List[Int])]) => x++=y )
      val joinac4 = joinac3.map(x=>(x._1, x._2.sortBy(y=>(y._1,y._2)).toArray))
      joinac4
    }

    def bisearch(chain:Array[(Int, Int, List[Int])], addlist:Array[List[Int]], lop:Int, rop:Int) : List[((Int, Int), Int)] = {
      var ans = List[((Int, Int), Int)]()
      val p=chain.size
      if (p==0) return ans
      if (p<20) {
        if (lop==0 && rop==0)
          for (adl <- addlist; c <- chain) {
            if ( adl(0)==c._1 && adl(1)<c._2) { for (k <- c._3) ans = ((adl(2), k), 1) :: ans}
            if ( adl(0)==c._1 && adl(1)>c._2) { for (k <- c._3) ans = ((adl(2), k), -1) :: ans}
          }
        if (lop==0 && rop==1)
          for (adl <- addlist; c <- chain) {
            if ( adl(0)==c._1 && adl(1)<=c._2) { for (k <- c._3) ans = ((adl(2), k), 1) :: ans}
            if ( adl(0)==c._1 && adl(1)>=c._2) { for (k <- c._3) ans = ((adl(2), k), -1) :: ans}
          }
        if (lop==0 && rop==2)
          for (adl <- addlist; c <- chain) {
            if ( adl(0)==c._1 && adl(1)<c._2) { for (k <- c._3) ans = ((adl(2), k), 1) :: ans}
            if ( adl(0)==c._1 && adl(1)>c._2) { for (k <- c._3) ans = ((adl(2), k), -1) :: ans}
          }
        if (lop==1 && rop==1)
          for (adl <- addlist; c <- chain) {
            if ( adl(0)>c._1 && adl(1)<=c._2) { for (k <- c._3) ans = ((adl(2), k), 1) :: ans}
            if ( adl(0)<c._1 && adl(1)>=c._2) { for (k <- c._3) ans = ((adl(2), k), -1) :: ans}
          }
        if (lop==1 && rop==2)
          for (adl <- addlist; c <- chain) {
            if ( adl(0)>c._1 && adl(1)<c._2) { for (k <- c._3) ans = ((adl(2), k), 1) :: ans}
            if ( adl(0)<c._1 && adl(1)>c._2) { for (k <- c._3) ans = ((adl(2), k), -1) :: ans}
          }
        if (lop==2 && rop==1)
          for (adl <- addlist; c <- chain) {
            if ( adl(0)>=c._1 && adl(1)<=c._2) { for (k <- c._3) ans = ((adl(2), k), 1) :: ans}
            if ( adl(0)<=c._1 && adl(1)>=c._2) { for (k <- c._3) ans = ((adl(2), k), -1) :: ans}
          }
        if (lop==2 && rop==2)
          for (adl <- addlist; c <- chain) {
            if ( adl(0)>=c._1 && adl(1)<c._2) { for (k <- c._3) ans = ((adl(2), k), 1) :: ans}
            if ( adl(0)<=c._1 && adl(1)>c._2) { for (k <- c._3) ans = ((adl(2), k), -1) :: ans}
          }
        return ans
      }
      for (adl <- addlist) {
        var i=0
        var j=p-1
        var m=0
        var dir=1
        if (chain(0)._1>adl(0)) {dir=1; m=0;}
        else if (chain(j)._1<adl(0)) {dir= -1; m=j;}
        else {
          while (j>i) {
            m=(j+i)/2
            if (chain(m)._1>=adl(0)) j=m
            else i=m+1
          }
          m=i
        }
        i=0
        j=p-1
        if (lop==0) {
          if (chain(m)._1==adl(0)) {
            if (rop==0 && chain(m)._2<adl(1)) { for (k <- chain(m)._3) ans = ((adl(2), k), 1) :: ans}
            else if (rop==0 && chain(m)._2>adl(1)) { for (k <- chain(m)._3) ans = ((adl(2), k), -1) :: ans}
            else if (rop==1 && chain(m)._2<=adl(1)) { for (k <- chain(m)._3) ans = ((adl(2), k), -1) :: ans}
            else if (rop==1 && chain(m)._2>=adl(1)) { for (k <- chain(m)._3) ans = ((adl(2), k), -1) :: ans}
            else if (rop==2 && chain(m)._2<adl(1)) { for (k <- chain(m)._3) ans = ((adl(2), k), -1) :: ans}
            else if (rop==2 && chain(m)._2>adl(1)) { for (k <- chain(m)._3) ans = ((adl(2), k), -1) :: ans}
          }
        }
        else if (lop==1) {
          if (chain(m)._1<adl(0)) dir = -1;
          else if (chain(m)._1==adl(0)) {
            if (chain(m)._2>adl(1)) {m=m-1; dir = -1;}
            else if (chain(m)._2==adl(1)) dir=0;
            else {m=m+1; dir=1;}
          }
          else {
            if (chain(m)._2>adl(1)) {m=m-1; dir= -1;}
            else if (chain(m)._2==adl(1)) dir=1;
            else dir=1;
          }
          if (dir==1 && rop==1) {
            while (m<=j && chain(m)._2<=adl(1)) {
              for (k<- chain(m)._3) ans = ((adl(2), k), 1) :: ans
              m=m+1
            }
          }
          else if (dir==1 && rop==2) {
            while (m<=j && chain(m)._2<adl(1)) {
              for (k<- chain(m)._3) ans = ((adl(2), k), 1) :: ans
              m=m+1
            }
          }
          else if (dir == -1 && rop==1){
            while (m>=i && chain(m)._2>=adl(1)) {
              for (k<- chain(m)._3) ans = ((adl(2), k), -1) :: ans
              m=m-1
            }
          }
          else if (dir == -1 && rop==2){
            while (m>=i && chain(m)._2>adl(1)) {
              for (k<- chain(m)._3) ans = ((adl(2), k), -1) :: ans
              m=m-1
            }
          }
        }
        else if (lop==2) {
          if (chain(m)._1<adl(0)) dir = -1;
          else if (chain(m)._1==adl(0)) {
            if (chain(m)._2>adl(1)) dir = -1;
            else if (chain(m)._2==adl(1)) {
              if (rop==1) {
                for (k<- chain(m)._3) ans = ((adl(2), k), -1) :: ans
                for (k<- chain(m)._3) ans = ((adl(2), k), 1) :: ans
              }
              dir=0;
            }
            else dir=1;
          }
          else {
            if (chain(m)._2>adl(1)) {m=m-1; dir = -1;}
            else if (chain(m)._2==adl(1)) dir=1;
            else dir=1;
          }
          if (dir==1 && rop==1) {
            while (m<=j && chain(m)._2<=adl(1)) {
              for (k<- chain(m)._3) ans = ((adl(2), k), 1) :: ans
              m=m+1
            }
          }
          else if (dir==1 && rop==2) {
            while (m<=j && chain(m)._2<adl(1)) {
              for (k<- chain(m)._3) ans = ((adl(2), k), 1) :: ans
              m=m+1
            }
          }
          else if (dir == -1 && rop==1){
            while (m>=i && chain(m)._2>=adl(1)) {
              for (k<- chain(m)._3) ans = ((adl(2), k), -1) :: ans
              m=m-1
            }
          }
          else if (dir == -1 && rop==2){
            while (m>=i && chain(m)._2>adl(1)) {
              for (k<- chain(m)._3) ans = ((adl(2), k), -1) :: ans
              m=m-1
            }
          }
        }
      }
      ans
    }

    val tf=sc.textFile(name, numpar).map(x=>x.split(","))
    val addlist = sc.textFile(nameadd, numparadd).map(x=>x.split(","))

    println( "Start POD 0 : [1>][2>]->[3>]");

    val pre1$0 = tf.map(x => ((x(1).toInt, x(2).toInt, x(3).toInt), x(0).toInt)).combineByKey(ArrayBuffer(_), (x:ArrayBuffer[Int], y:Int) => x+=y, (x:ArrayBuffer[Int], y:ArrayBuffer[Int]) => x++=y )
    val pre2_0 = pre1$0.map(x=>{x._2.sortBy(x=>x); val len=x._2.length; for (i<- 0 until len) println("PRESS "+x._2(0)+" "+x._2(i)); List(x._2(0), x._1._1, x._1._2, x._1._3)})

    val chain_0_1 = makechain(pre2_0, sizeofhash, 1, 3, numpar, 1.9996470241, 447.9627612045)
    val chain_0_2 = makechain(pre2_0, sizeofhash, 2, 3, numpar, 2.0003531127, 448.0082303581)

    val add_0_1 = addlist.map(x=> List[Int](x(1).toInt, x(3).toInt, x(0).toInt)).collect
    val add_0_2 = addlist.map(x=> List[Int](x(2).toInt, x(3).toInt, x(0).toInt)).collect

    val set_0_1 = chain_0_1.flatMap(x=> bisearch(x._2, add_0_1, 1, 1))
    val set_0_2 = chain_0_2.flatMap(x=> bisearch(x._2, add_0_2, 1, 1))

    val filpos_0_1 = set_0_1.filter(x => x._2 == 1)
    val filneg_0_1 = set_0_1.filter(x => x._2 == -1)
    val filpos_0_2 = set_0_2.filter(x => x._2 == 1)
    val filneg_0_2 = set_0_2.filter(x => x._2 == -1)

    val anspos_0 = filpos_0_1.join(filpos_0_2).collect
    anspos_0.foreach(x=> println("VIO "+x._1._1+" "+x._1._2))

    val ansneg_0 = filneg_0_1.join(filneg_0_2).collect
    ansneg_0.foreach(x=> println("VIO "+x._1._1+" "+x._1._2))

    println( "End POD 0 : [1>][2>]->[3>]");

  }

}
