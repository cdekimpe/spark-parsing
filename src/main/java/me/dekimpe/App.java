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
        lines = lines.filter(s -> s.startsWith("INSERT INTO")); // Select lines that start only with 'INSERT INTO'
        lines = lines.filter(s -> s.substring(31)); // Substract 'INSERT INTO `pagelinks` VALUES ' from the line
        JavaPairRDD values = JavaPairRDD.fromJavaRDD(lines.map(s -> getValues(s)));
        values.collect();
        
        System.out.println(values);
        
        System.out.println("Hello World!");
    }
    
    private static HashMap<Integer, String> getValues(String s) {
        
        // Exceptions :
        // (936086,0,'\'Midst_Woodland_Shadows',0)
        // (11899918,0,'(1)_Cérès',0)
        
        int i = 0;
        int pl_from = 0;
        String temp;
        String pl_title;
        String[] comp = s.split(",");
        int totalCount = comp.length;
        HashMap<Integer,String> result = new HashMap<>();
        for (int u = 0; u < totalCount; u = u+2) {
            if (u%4 == 0) {
                temp = comp[u].substring(1);
                pl_from = Integer.parseInt(temp);
            } else if (u%4 == 2) {
                pl_title = comp[u].substring(1, comp[u].length() -1);
                result.put(pl_from, pl_title);
            }
            if (u >= 100)
                break;
        }
        
        return result;
    }
}
