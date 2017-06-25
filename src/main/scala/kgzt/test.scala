package kgzt
import org.apache.spark.{SparkContext, SparkConf}
/**
  * Created by liuxw on 2017/6/3.
  */
object test {
  def main(args: Array[String]) {

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


