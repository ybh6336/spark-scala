package learningspark.chapter2

import org.apache.spark.SparkContext

object LineFilter {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "LineFilter", System.getenv("SPARK_HOME"))
    val linesRDD = sc.textFile("file:///development/git-repos/spark-scala/src/main/scala/learningspark/chapter2/README.txt")
    val scalaLinesRDD = linesRDD.filter((line) => line.contains("Scala"))

    print(scalaLinesRDD.first())
  }
}
