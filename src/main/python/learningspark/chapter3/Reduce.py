# export PYSPARK_PYTHON=/usr/bin/python
# spark-submit Reduce.py
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('Reduce python')
sc = SparkContext(conf = conf)

rdd = sc.parallelize([1, 2, 3, 4, 5])
reduced = rdd.reduce(lambda val1, val2: val1 + val2)

print 'reduced = {0}'.format(reduced)
