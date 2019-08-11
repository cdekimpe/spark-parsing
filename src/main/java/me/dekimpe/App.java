package me.dekimpe;

import java.util.HashMap;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
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
        
        JavaRDD<String> lines = sc.textFile("hdfs://192.168.10.14:9000/pagelinks-sql/pagelinks.sql");
        lines = lines.filter(s -> s.startsWith("INSERT INTO"));
        //JavaPairRDD values = JavaPairRDD.fromJavaRDD(lines.map(s -> getValues(s)));
        
        //JavaRDD<String> words = textFile.flatMap(LineIterator::new);
        
        
        System.out.println("Hello World!");
    }
    
    private static List<HashMap<Integer, String>> getValues(String s) {
        
    }
}
