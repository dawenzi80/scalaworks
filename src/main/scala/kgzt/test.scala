package kgzt
import org.apache.spark.{SparkContext, SparkConf}
/**
  * Created by liuxw on 2017/6/3.
  */
object test {
  def main(args: Array[String]) {
    println("Hello World!")
    val sparkConf = new SparkConf().setMaster("local").setAppName("test")
    val sparkContext = new SparkContext(sparkConf)
    val pattern = new Regex("imsi=(\\d+)&")
    val str = "vNlLjsur&imei=863798020399098&appKey=umeng%3A4f8d2b825270157c0e00008c&v=6.0&sign=914e1041734f758ce046ebf0128ec2b6&data=%7B%22c1%22%3A%22ZTE+Grand+S+II+LTE%22%2C%22c2%22%3A%22863798020399098%22%2C%22agooSdkVersion%22%3A20131220%2C%22vote%22%3A%22remote%22%2C%22c0%22%3A%22ZTE%22%2C%22c6%22%3A%22367fb57fb18a5f1d%22%2C%22c5%22%3A%2251096e0d%22%2C%22c4%22%3A%229c%3Aa9%3Ae4%3A5c%3Ae2%3A02%22%2C%22c3%22%3A%22460000950798056%22%2C%22appPackage%22%3A%22com.hf%22%2C%22deviceId%22%3A%22AtBBpZPvKb2VvK5F0tDrP92BimbBB2bQz9hEvNlLjsur%22%7D&api=mtop.push.msg.new&imsi=460000950798056&nt=wifi&lac=9437&cid=40901"
    println((pattern findAllIn str).mkString(","))

    //TEST  TUPLE
    val c=Array(2,3,5,7,11)
    //
    val result = for(elem<-c) yield elem*2
    println("result:")
    for(elem<-result) println(elem)
    //
    val result2 = for(elem<-c if elem%2==0) yield elem*2
    println("result 2:")
    for(elem<-result2) println(elem)

    //result3和result2的结果是一样的
    val result3 = c.filter(_%2==0).map(2*_)
    println("result 3:")
    for(elem<-result3) println(elem)
    //求和
    println(c.sum)
    //最大值
    println(c.max)
    //
    val result4=c.sorted;
    println("result 4:")
    for(elem<-result4) println(elem)
    //

    //val sparkConf = new SparkConf().setMaster("local").setAppName("test")
    //sval sparkContext = new SparkContext(sparkConf)
  }
}


