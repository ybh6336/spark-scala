package learningspark.chapter2

import org.apache.spark.SparkContext

object WordCount {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "WordCount", System.getenv("SPARK_HOME"))
    val linesRDD = sc.textFile("file:///development/git-repos/spark-scala/src/main/scala/learningspark/chapter2/README.txt")

    val wordCounts = linesRDD.flatMap(line => line.split(" "))
                             .map(word => (word, 1))
                             .reduceByKey((count1, count2) => count1 + count2)

    // use collect() to iterate over the tuples and print values
    // NOTE: collect() would bring back all values to the Driver JVM and runs
    // the risk of causing java.lang.OutOfMemoryError
//    wordCounts.collect()
//              .foreach(wordCountTuple => println(wordCountTuple._1 + ": " + wordCountTuple._2))

    wordCounts.saveAsTextFile("file:///development/git-repos/spark-scala/src/main/scala/learningspark/chapter2/word-count-output.txt")
  }
}
