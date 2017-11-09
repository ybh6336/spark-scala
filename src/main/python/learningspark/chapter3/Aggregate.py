# export PYSPARK_PYTHON=/usr/bin/python
# spark-submit Aggregate.py
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('Aggregate python')
sc = SparkContext(conf = conf)

rdd = sc.parallelize([1, 2, 3, 4, 5])
aggregated = rdd.aggregate(
    (0, 0),
    lambda acc, val: (acc[0] + val, acc[1] + 1),
    lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1]))

result = aggregated[0] / aggregated[1]

print 'aggregated = {0}'.format(result)
