package kgzt;

/**
 * Created by liuxw on 2017/6/3.
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.storage.StorageLevel;

public class JavaSpark {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.WARN);
        String dataFile = "d:/readme.md";
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Simple App");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> data = sparkContext.textFile(dataFile).persist(StorageLevel.MEMORY_ONLY());
        JavaRDD<String> adata = data.filter((String str) -> str.contains("a"));
        adata.saveAsTextFile("d:\\aread.txt");
        long numAs = data.filter((String str) -> str.contains("a")).count();
        long numBs = data.filter((String str) -> str.contains("b")).count();
        System.out.println("Total Lines : " + data.count() );
        System.out.println("Lines with a: " + numAs );
        System.out.println("Lines with b: " + numBs );

    }
}