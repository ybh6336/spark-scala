package learningspark.chapter3;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;

public class ReduceFoldAggregateJ {
  public static void main(String... args) {
    JavaSparkContext sc = new JavaSparkContext("local", "ReduceFoldAggregateJ");

    reduce(sc);

    fold(sc);

    aggregate(sc);
  }

  private static void reduce(JavaSparkContext sc) {
    JavaRDD rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));

    @SuppressWarnings("unchecked")
    Integer reduced = (Integer) rdd.reduce(new Function2<Integer, Integer, Integer>() {
      public Integer call(Integer val1, Integer val2) {
        return val1 + val2;
      }
    });

    System.out.println("reduced = " + reduced);
  }

  private static void fold(JavaSparkContext sc) {
    JavaRDD rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));

    @SuppressWarnings("unchecked")
    Integer folded = (Integer) rdd.fold(
            0,
            new Function2<Integer, Integer, Integer>() {
              public Integer call(Integer acc, Integer val) {
                return acc + val;
              }
            });

    System.out.println("folded = " + folded);
  }

  private static void aggregate(JavaSparkContext sc) {
    JavaRDD rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));

    @SuppressWarnings("unchecked")
    Tuple2<Integer, Integer> aggregated = (Tuple2<Integer, Integer>) rdd.aggregate(
            new Tuple2<Integer, Integer>(0, 0),
            new Function2<Tuple2, Integer, Tuple2>() {
              @Override
              public Tuple2<Integer, Integer> call(Tuple2 acc, Integer value) {
                Integer updatedValue = ((Integer) acc._1()) + value;
                Integer updatedCount = ((Integer) acc._2()) + 1;
                return new Tuple2<>(updatedValue, updatedCount);
              }
            },
            new Function2<Tuple2, Tuple2, Tuple2>() {
              @Override
              public Tuple2<Integer, Integer> call(Tuple2 acc1, Tuple2 acc2) {
                Integer combinedValue = ((Integer) acc1._1()) + ((Integer) acc2._1());
                Integer combinedCount = ((Integer) acc1._2()) + ((Integer) acc2._2());
                return new Tuple2<>(combinedValue, combinedCount);
              }
            });

    Double result = aggregated._1().doubleValue() / aggregated._2().doubleValue();
    System.out.println("aggregated = " + result);
  }
}
