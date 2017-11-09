# export PYSPARK_PYTHON=/usr/bin/python
# spark-submit Fold.py
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('Fold python')
sc = SparkContext(conf = conf)

rdd = sc.parallelize([1, 2, 3, 4, 5])
folded = rdd.fold(0, lambda acc, val: acc + val)

print 'folded = {0}'.format(folded)
