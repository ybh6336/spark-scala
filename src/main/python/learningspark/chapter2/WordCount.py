# export PYSPARK_PYTHON=/usr/bin/python
# spark-submit WordCount.py
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('WordCount python')
sc = SparkContext(conf = conf)

linesRDD = sc.textFile("file:///development/git-repos/spark-scala/src/main/scala/learningspark/chapter2/README.txt")

wordsRDD = linesRDD.flatMap(lambda line: line.split(' ')) \
                   .map(lambda word: (word, 1)) \
                   .reduceByKey(lambda count1, count2: count1 + count2)

wordsRDD.saveAsTextFile("file:///development/git-repos/spark-scala/src/main/scala/learningspark/chapter2/word-count-output.txt")
