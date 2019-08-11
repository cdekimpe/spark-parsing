package me.dekimpe;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        SparkConf conf = new SparkConf()
                .setAppName("Spark Parsing")
                .setMaster("spark://192.168.10.14:7077");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        JavaRDD<String> textFile = sc.textFile("hdfs://192.168.10.14:9000/pagelinks-sql/pagelinks.sql");
        //JavaRDD<String> words = textFile.flatMap(LineIterator::new);
        
        
        System.out.println("Hello World!");
    }
}
