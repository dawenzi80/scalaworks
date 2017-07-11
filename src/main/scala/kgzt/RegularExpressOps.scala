package kgzt

import scala.util.matching.Regex

/**
  * Created by liuxw on 2017/7/7.
  */
object RegularExpressOps {
  def main(args:Array[String]):Unit={
    val regex="""([0-9]+)([a-z]+)""".r//"""原生表达
    val numPattern="[0-9]+".r
    val numberPattern="""\s+[0-9]+\s+""".r



    //findAllIn()方法返回遍历所有匹配项的迭代器
    println("1st match...")
    for(matchString <- numPattern.findAllIn("99345 Scala,22298 Spark"))
      println(matchString)

    //找到首个匹配项
    println("2nd match...")
    for(matchString <- numberPattern.findAllIn("99ss java, 222 spark, 333 hadoop"))
      println(matchString)
   // println(numberPattern.findFirstIn("99ss java, 222 spark,333 hadoop"))
    //imei测试
    println("2nd match...")
    val imeiLine="vNlLjsur&imei=863798020399098&appKey=umeng%3A4f8d2b825270157c0e00008c&v=6.0&sign=914e1041734f758ce046ebf0128ec2b6&data=%7B%22c1%22%3A%22ZTE+Grand+S+II+LTE%22%2C%22c2%22%3A%22863798020399098%22%2C%22agooSdkVersion%22%3A20131220%2C%22vote%22%3A%22remote%22%2C%22c0%22%3A%22ZTE%22%2C%22c6%22%3A%22367fb57fb18a5f1d%22%2C%22c5%22%3A%2251096e0d%22%2C%22c4%22%3A%229c%3Aa9%3Ae4%3A5c%3Ae2%3A02%22%2C%22c3%22%3A%22460000950798056%22%2C%22appPackage%22%3A%22com.hf%22%2C%22deviceId%22%3A%22AtBBpZPvKb2VvK5F0tDrP92BimbBB2bQz9hEvNlLjsur%22%7D&api=mtop.push.msg.new&imsi=460000950798056&nt=wifi&lac=9437&cid=40901";
    val agentLine="Dalvik/1.6.0 (Linux; U; Android 4.4.4; Che1-CL10 Build/Che1-CL10) SohuNewsSDK/32";
    val rimei = new Regex("""imei=(\d+)&""", "g1").findFirstMatchIn(imeiLine) // with named groups
    val rimsi = new Regex("""imsi=(\d+)&""", "g1").findFirstMatchIn(imeiLine) // with named groups
    val rimmi = new Regex("""immi=(\d+)&""", "g1").findFirstMatchIn(imeiLine) // with named groups
    val rmodel = new Regex(""";([^;]+) Build/""", "g1").findFirstMatchIn(agentLine) // with named groups
    println("imsi= \t" + (if(rimsi.nonEmpty) rimsi.get.group("g1") else ""))
    println("imei=  \t" +(if(rimei.nonEmpty) rimei.get.group("g1") else ""))
    println("immi=  \t" +(if(rimmi.nonEmpty) rimmi.get.group("g1") else ""))
    println("model=  \t" +(if(rmodel.nonEmpty) rmodel.get.group("g1").trim else ""))

    //println("imei= \t" + (r4 findFirstMatchIn imeiLine).get.group("g1"))
    ////
    val imeiPattern="""(imei=)([0-9]+)(&)""".r
    for(imeiPattern(prefix, imei,postfix) <- imeiPattern.findAllIn(imeiLine))
      println("imei:\t" + imei)

    //数字和字母的组合正则表达式
    val numitemPattern="""([0-9]+) ([a-z]+)""".r

    val numitemPattern(num, item)="99 hadoop"

    val line="93459 hspark"
    println("3rd match...")
    line match{
      case numitemPattern(num,blog)=> println(num+"--\t--"+blog)
      case _=>println("hahaha...")
    }
  }
}
