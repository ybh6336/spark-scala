package learningspark.chapter2;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;

public class LineCountJ {

  public static void main(String[] args) {
    SparkContext sc = new SparkContext("local", "LineCountJ", System.getenv("SPARK_HOME"));
    JavaRDD<String> linesRDD = sc.textFile("file:///development/git-repos/spark-scala/src/main/scala/learningspark/chapter2/README.txt", 1).toJavaRDD();

    System.out.println(linesRDD.count());
  }
}
