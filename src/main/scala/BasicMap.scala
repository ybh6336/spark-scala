import org.apache.spark.SparkContext

object BasicMap {

  def main(args: Array[String]) : Unit = {
    val sc = new SparkContext("local", "BasicMap", System.getenv("SPARK_HOME"));
    val input = sc.parallelize(List(1, 2, 3, 4));
    val result = input.map(x => x * x);
    println(result.collect().mkString(","));
  }
}
