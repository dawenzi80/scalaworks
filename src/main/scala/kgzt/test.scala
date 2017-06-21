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
  }
}


