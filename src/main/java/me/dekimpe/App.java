package me.dekimpe;

import me.dekimpe.types.PageLink;
import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

public class App 
{

    public static void main(String[] args)
    {
        
        SparkConf conf = new SparkConf()
                .set("spark.executor.extraClassPath", "/home/hadoop/*:")
                .setAppName("Spark Parsing SQL - Context")
                .setMaster("spark://192.168.10.14:7077");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        SparkSession spark = SparkSession
                .builder()
                .appName("Spark Parsing SQL - Session")
                .master("spark://192.168.10.14:7077")
                //.config("spark.some.config.option", "some-value")
                .getOrCreate();

        JavaRDD<PageLink> lines = sc.textFile("hdfs://hdfs-namenode:9000/input/" + args[0])
                .filter(s -> s.startsWith("INSERT INTO")) // Only INSERT INTO lines
                .map(s -> s.substring(31)) // Substract 'INSERT INTO `pagelinks` VALUES ' from the line
                .flatMap(s -> Arrays.asList(s.split("\\),\\(")).iterator())
                .map(s -> getValues(s));
        
        Dataset<Row> df = spark.createDataFrame(lines, PageLink.class);
        df.write().mode(SaveMode.Overwrite).format("avro").save("hdfs://hdfs-namenode:9000/schemas/" + args[1]);
        
        //System.out.println("Total : " + df.count());
        //System.out.println("Fakes : " + df.filter("title = 'faaaakeOne'").count());
    }
    
    private static PageLink getValues(String s) {

        PageLink pageLink = new PageLink();
        pageLink.setId(-1);
        pageLink.setTitle("");
        if(!s.contains(","))
            return pageLink;
        String[] comp = s.split(",");
        if (comp.length < 4)
            return pageLink;
        try {
            pageLink.setId(Integer.parseInt(comp[0].substring(1)));
        } catch (NumberFormatException e) {
            try {
                pageLink.setId(Integer.parseInt(comp[0]));
            } catch (NumberFormatException e2) {
                System.err.println("Error parsing id : String = '" + s);
            }
        }
        if (comp[2].length() >= 2)
            pageLink.setTitle(comp[2].substring(1, comp[2].length() - 1));
        else
            System.err.println("Error parsing title : String = '" + s);
        return pageLink;

    }
    
}
