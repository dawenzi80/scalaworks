package kgzt
/* SimpleApp.scala */
import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import scala.util.matching.Regex

object Test2 {
  def main(args: Array[String]) {

    //默认HDFS,本地读取需要加：file://
    val logFile = "E:\\FtpSite\\log\\*" // Should be some file on your system
    val conf = new SparkConf().setMaster("local").setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val logDataRdd = sc.textFile(logFile, 2).cache()
    //
    val rimei = new Regex("""imei=(\d+)&""", "g1") // with named groups
    val rimsi = new Regex("""imsi=(\d+)&""", "g1")//
    val rmodel = new Regex(""";([^;]+) Build/""", "g1") //
    //
    logDataRdd.foreach(line=>{
      val aLineSeg= line.split("\t")
      val cusid=(if(aLineSeg.length>4) aLineSeg(1))
      val mac=(if(aLineSeg.length>4) aLineSeg(2))
      val mimei  = rimei.findFirstMatchIn(line)
      val mimsi  = rimsi.findFirstMatchIn(line)
      val mmodel = rmodel.findFirstMatchIn(line)
      //if(mimei.nonEmpty) println(cusid+"\t"+mac+":\t"+mimei.get.group("g1"))
      //if(mimsi.nonEmpty) println(cusid+"\t"+mac+":\t"+mimsi.get.group("g1"))
      //if(mmodel.nonEmpty) println(cusid+"\t"+mac+":\t"+mmodel.get.group("g1"))
    }
    )

    def myfunc[T](iter: Iterator[T]) : Iterator[(T, T)] = {
      var res = List[(T, T)]()
      while (iter.hasNext) {
        val line = iter.next.toString;

        val aLineSeg= line.split("\t")
        val cusid=(if(aLineSeg.length>4) aLineSeg(1))
        val mac=(if(aLineSeg.length>4) aLineSeg(2))
        val mimei  = rimei.findFirstMatchIn(line)
        val mimsi  = rimsi.findFirstMatchIn(line)
        val mmodel = rmodel.findFirstMatchIn(line)
        val imei =(if(mimei.nonEmpty) mimei.get.group("g1") else "")
        val imsi =(if(mimsi.nonEmpty) mimsi.get.group("g1") else "")
        val model =(if(mmodel.nonEmpty) mmodel.get.group("g1") else "")
       // println(mac+"\t"+model)
        res.::(mac,imsi)
      }
      println("size1:"+res.size)
      res.iterator
    }


    val rddimei = logDataRdd.map(line=>(
      {
      val aLineSeg = line.split("\t")
      if (aLineSeg.length > 4) aLineSeg(1)+"--"+aLineSeg(2) else ""
       } ,
      {
        val mimei  = rimei.findFirstMatchIn(line)
        if(mimei.nonEmpty) mimei.get.group("g1") else ""
      }
    )).distinct().filter(f=>(f._2!="")).map(f=>(f._1,1)).reduceByKey((x,y)=>x+y)
    ////
    val rddimsi = logDataRdd.map(line=>(
      {
        val aLineSeg = line.split("\t")
        if (aLineSeg.length > 4) aLineSeg(1)+"--"+aLineSeg(2) else ""
      } ,
      {
        val mimsi  = rimsi.findFirstMatchIn(line)
        if(mimsi.nonEmpty) mimsi.get.group("g1") else ""
      }
    )).distinct().filter(f=>(f._2!="")).map(f=>(f._1,1)).reduceByKey((x,y)=>x+y)

    ///
    val rddmodel = logDataRdd.map(line=>(
      {
        val aLineSeg = line.split("\t")
        if (aLineSeg.length > 4) aLineSeg(1)+"--"+aLineSeg(2) else ""
      } ,
      {
        val mmodel  = rmodel.findFirstMatchIn(line)
        if(mmodel.nonEmpty) mmodel.get.group("g1") else ""
      }
    )).distinct().filter(f=>(f._2!="")).map(f=>(f._1,1)).reduceByKey((x,y)=>x+y)

    ///
    val rddall = logDataRdd.map(line=>(
      {
        val aLineSeg = line.split("\t")
        if (aLineSeg.length > 4) aLineSeg(1)+"--"+aLineSeg(2) else ""
      } ,
      {
        val mimei  = rimei.findFirstMatchIn(line)
        val mimsi  = rimsi.findFirstMatchIn(line)
        val mmodel = rmodel.findFirstMatchIn(line)
        val imei =(if(mimei.nonEmpty) mimei.get.group("g1") else "")
        val imsi =(if(mimsi.nonEmpty) mimsi.get.group("g1") else "")
        val model =(if(mmodel.nonEmpty) mmodel.get.group("g1") else "")
        (imei,imsi,model)
      }
    )).distinct().reduceByKey((x,y)=>({if(x._1=="") y._1 else  x._1},{if(x._2=="") y._2 else  x._2},{if(x._3=="") y._3 else  x._3})).filter(f=>{f._2._1!="" || f._2._2!="" ||f._2._1!=""})

  ////
    println("size imsi:"+rddimsi.count())
    println("size imei:"+rddimei.count())
    println("size model:"+rddmodel.count())

    rddimsi.collect().foreach(println(_))
    rddimei.collect().foreach(println(_))
    rddmodel.collect().foreach(println(_))
    rddall.collect().foreach(println(_))
//
    println("Total lines : %s".format(logDataRdd.count()))
    val numAs = logDataRdd.filter(line => line.contains("imei")).count()
    val numBs = logDataRdd.filter(line => line.contains("imsi")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
    //
    val outputFile: File = new File("d:\\output.txt")
    if (outputFile.exists) util.MyUtils.DelFileOrFolder("d:\\output.txt")
    rddall.saveAsTextFile("d:\\output.txt")
  }
}
