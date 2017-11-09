package learningspark.chapter2

import org.apache.spark.SparkContext

object LineCount {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "LineCount", System.getenv("SPARK_HOME"))
    val linesRDD = sc.textFile("file:///development/git-repos/spark-scala/src/main/scala/learningspark/chapter2/README.txt")
    println(linesRDD.count())
  }
}
