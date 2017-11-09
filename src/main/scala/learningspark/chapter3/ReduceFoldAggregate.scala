package learningspark.chapter3

import org.apache.spark.SparkContext

object ReduceFoldAggregate {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "Reduce", System.getenv("SPARK_HOME"))

    reduce(sc)

    fold(sc)

    aggregate(sc)
  }

  def reduce(sc: SparkContext): Unit = {
    val rdd = sc.parallelize(List(1, 2, 3, 4, 5))
    val reduced = rdd.reduce((val1, val2) => val1 + val2)

    println("reduced = " + reduced)
  }

  def fold(sc: SparkContext): Unit = {
    val rdd = sc.parallelize(List(1, 2, 3, 4, 5))
    val folded = rdd.fold(0) ((val1, val2) => val1 + val2)

    println("folded = " + folded)
  }

  def aggregate(sc: SparkContext): Unit = {
    val rdd = sc.parallelize(List(1, 2, 3, 4, 5))
    val aggregated = rdd.aggregate((0, 0)) (
      (acc, value) => (acc._1 + value, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    val result = aggregated._1 / aggregated._2.toDouble

    println("aggregated = " + result)
  }
}
