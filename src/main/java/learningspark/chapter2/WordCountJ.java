package learningspark.chapter2;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

public class WordCountJ {

  public static void main(String... args) {
    SparkContext sc = new SparkContext("local", "WordCountJ", System.getenv("SPARK_HOME"));
    JavaRDD<String> linesRDD = sc.textFile("file:///development/git-repos/spark-scala/src/main/scala/learningspark/chapter2/README.txt", 1).toJavaRDD();

    JavaRDD<String> wordsRDD = linesRDD.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterable<String> call(String line) throws Exception {
        return Arrays.asList(line.split(" "));
      }
    });

    JavaPairRDD<String, Integer> wordCountsRDD = wordsRDD.mapToPair(new PairFunction<String, String, Integer>() {
      @Override
      public Tuple2<String, Integer> call(String word) throws Exception {
        return new Tuple2<>(word, 1);
      }
    }).reduceByKey(new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer count1, Integer count2) throws Exception {
        return count1 + count2;
      }
    });

    wordCountsRDD.saveAsTextFile("file:///development/git-repos/spark-scala/src/main/scala/learningspark/chapter2/word-count-output.txt");
  }
}
