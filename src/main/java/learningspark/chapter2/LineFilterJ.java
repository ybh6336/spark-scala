package learningspark.chapter2;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

public class LineFilterJ {

  public static void main(String... args) {
    SparkContext sc = new SparkContext("local", "LineFilterJ", System.getenv("SPARK_HOME"));
    JavaRDD<String> linesRDD = sc.textFile("file:///development/git-repos/spark-scala/src/main/scala/learningspark/chapter2/README.txt", 1).toJavaRDD();
    JavaRDD<String> filteredLinesRDD = linesRDD.filter(new Function<String, Boolean>() {
      @Override
      public Boolean call(String line) {
        return line.contains("Java");
      }
    });

    System.out.println(filteredLinesRDD.first());
  }
}
