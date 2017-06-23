package kgzt;

/**
 * Created by liuxw on 2017/6/3.
 */

import kgzt.kgzt.model.QztReportLog;
import kgzt.util.MyLogger;
import kgzt.util.MyUtils;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.File;
import java.io.Serializable;

public class JavaSpark implements Serializable {
    public static void main(String[] args) {
        MyLogger.LOGGER.setLevel(Level.WARN);
        //一般的语法和功能验证测试
        //new JavaSpark().generalTest();
        //统计Log文件的手机型号信息
        //new JavaSpark().analysisQztLogByModel();
        //
        new JavaSpark().analysisQztLogByRouter();
    }

    /**
     * 一般的语法和功能验证测试
     */
    protected void generalTest() {
        String dataFile = "d:/readme.md*";
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Simple App");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> data = sparkContext.textFile(dataFile).persist(StorageLevel.MEMORY_ONLY());
        JavaRDD<String> adata = data.filter((String str) -> str.contains("dog"));
        //delete output file if file exists already
        File outputFile = new File("d:\\output.txt");
        if (outputFile.exists())
            MyUtils.DelFileOrFolder("d:\\output.txt");
        //
        adata.saveAsTextFile("d:\\output.txt");
        long numAs = data.filter((String str) -> str.contains("a")).count();
        long numBs = data.filter((String str) -> str.contains("dog")).count();
        System.out.println("Total Lines : " + data.count());
        System.out.println("Lines with a: " + numAs);
        System.out.println("Lines with b: " + numBs);
    }

    /**
     * 使用企智通的日志文件，做统计方面的测试
     * 统计手机型号
     */
    protected void analysisQztLogByModel() {
        String dataFile = "E:\\FtpSite\\test\\20160331\\database\\*";
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Simple App");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> logDataRdd = sparkContext.textFile(dataFile).persist(StorageLevel.MEMORY_AND_DISK());
        //for each,method 1
        logDataRdd.foreach(new VoidFunction<String>() {
            public void call(String s) {
                //System.out.println(s);
            }
        });
        //for each,method 2
        //logDataRdd.foreach( a -> System.out.println("======"+a));
        //map method 1，取得每行中的手机型号作为key
        JavaPairRDD<String, Integer> pairRdd = logDataRdd.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String line) throws Exception {
                        /*
                        String method =  line.split("\t")[0].trim();
                        if(method.equalsIgnoreCase("GET") || method.equalsIgnoreCase("post")||method.equalsIgnoreCase("post2"))
                        {}
                        else {
                            method = "null";
                        }*/
                        String model = getPhoneModel(line);
                        return new Tuple2<String, Integer>(model, 1);
                    }
                });
        //reduce，统计每种方法出现的次数
        JavaPairRDD result = pairRdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer x, Integer y) throws Exception {
                return x + y;
            }
        });
    //按照数量排序,因为JavaPairRDD只提供了按照key进行排序的方法，因此，先把key和value进行交换，按照key排序，再将结果进行key
        result = result.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> item) throws Exception {
                return item.swap();
            }
        });
        result = result.sortByKey();
        result = result.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> item) throws Exception {
                return item.swap();
            }
        });
        //保存文件和打印
        System.out.println(result.collect());
        saveToFile(result,"d:\\output.txt");
        System.out.println("---------------------------------------");
        System.out.println(logDataRdd.count());
        //
    }

    /**
     * 使用企智通的日志文件，做统计方面的测试
     * 根据mac地址下的手机型号数量，来判断路由器
     * 输出的结果是具体的mac下面的手机型号的数量和具体的内容
     */
    protected void analysisQztLogByRouter() {
        String dataFile = "E:\\FtpSite\\log\\*";
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Simple App");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> logDataRdd = sparkContext.textFile(dataFile).persist(StorageLevel.MEMORY_AND_DISK());
        //map method 1，取得每行中从日志中提取场所id，和mac拼接起来作为key，value是手机型号
        JavaPairRDD<String, String> pairRdd = logDataRdd.mapToPair(
                new PairFunction<String, String, String>() {
                    public Tuple2<String, String> call(String line) throws Exception {
                        return getRouterTuple(line);
                    }
                });
        System.out.println("rdd count:"+pairRdd.count());
        //去重
        pairRdd = pairRdd.distinct();
        System.out.println("rdd count after distinct:"+pairRdd.count());
        System.out.println(pairRdd.collect());

        //统计去重后的数量
        JavaPairRDD result  = pairRdd.mapToPair(new PairFunction< Tuple2<String, String>, String, QztReportLog>() {
            @Override
            public Tuple2<String ,QztReportLog> call(Tuple2<String, String> item) throws Exception {

                return  new Tuple2<String , QztReportLog>(item._1(),new QztReportLog(item._2()) );
            }
        });
        System.out.println("result.collect:");
        System.out.println(result.collect());

        //reduce，统计每个mac下出现的手机型号的出现次数
        result = result.reduceByKey(new Function2<QztReportLog, QztReportLog, QztReportLog>() {
            public QztReportLog call(QztReportLog x, QztReportLog y) throws Exception {
                QztReportLog log = new QztReportLog(x.getModel()+"-"+y.getModel());
                log.setNum(x.getNum()+y.getNum());
                return log;
            }
        });
        //按照数量排序,因为JavaPairRDD只提供了按照key进行排序的方法，因此，先把key和value进行交换，按照key排序，再将结果进行key
        result = result.mapToPair(new PairFunction<Tuple2<String, QztReportLog>, QztReportLog, String>() {
            @Override
            public Tuple2<QztReportLog, String> call(Tuple2<String, QztReportLog> item) throws Exception {
                return item.swap();
            }
        });
        result = result.sortByKey();
        result = result.mapToPair(new PairFunction<Tuple2<QztReportLog, String>, String, QztReportLog>() {
            @Override
            public Tuple2<String, QztReportLog> call(Tuple2<QztReportLog, String> item) throws Exception {
                return item.swap();
            }
        });
        //保存文件和打印
        System.out.println(result.collect());
        saveToFile(result,"d:\\output.txt");
        System.out.println("---------------------------------------");
        System.out.println(logDataRdd.count());

    }

    /**
     * 从日志中提取手机型号
     * @param line
     * @return
     */
    private String getPhoneModel( String line)
    {
        String phoneModel = "";
        StringBuffer sbf  = new StringBuffer(line).reverse();
        int begin = sbf.indexOf("/dliuB");
        if(begin>0)
        {
            int end = sbf.indexOf(";",begin);
            if(end>0)
            {
                phoneModel = new StringBuffer(sbf.substring(begin+6,end)).reverse().toString();
            }
        }
        return phoneModel;
    }
    /**
     * 从日志中提取场所id，和mac拼接起来作为key，value是手机型号
     * id是每一行的第二个字段，mac是第三个字段
     * @param line
     * @return
     */
    private Tuple2<String, String> getRouterTuple( String line)
    {
        String key  = "";
        String httpinfo[] = line.split("\t");
        if(httpinfo.length>3)
        {
            key =  httpinfo[1]+"_"+httpinfo[2];
        }
        String phoneModel = getPhoneModel(  line);

        return new Tuple2<String, String>(key, phoneModel);
    }
    /**
     * 保存rdd
     * @param rdd
     * @param filePathName
     */
    private void saveToFile( JavaPairRDD rdd,String filePathName)
    {
        //delete output file if file exists already
        File outputFile = new File(filePathName);
        if (outputFile.exists())
            MyUtils.DelFileOrFolder(filePathName);
        //
        rdd.coalesce(1,true).saveAsTextFile(filePathName);
    }
}