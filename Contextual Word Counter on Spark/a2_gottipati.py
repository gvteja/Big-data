# _*_ coding:utf-8 _*_
import re
from pyspark import SparkContext
from os import path

feel_words_regex = re.compile(r'(?<=\bi feel\s)(\w+)')
def extractWordsRegex(line):
    counts = {}
    line = line.lower()
    next_words = re.findall(feel_words_regex, line)
    for next_word in next_words:
        if not next_word:
            continue
        try:
            next_word.encode('ascii', errors='strict')
            counts[next_word] = counts.get(next_word, 0) + 1
        except:
            continue

    return counts.items()

def computeCounts(rdd):
    return rdd.flatMap(extractWordsRegex)\
        .reduceByKey(lambda x,y: x + y)


wet_path = 's3://commoncrawl/crawl-data/CC-MAIN-2016-40/wet.paths.gz'
cc_prefix = 's3://commoncrawl/'
my_bucket = 's3://vijay-bd'
# base_dir = '/Users/bobby/Downloads'
# wet_path = path.join(base_dir, 'tmp.txt')
# cc_prefix = path.join(base_dir, 'crawl-data')
# my_bucket = base_dir

sc = SparkContext(appName="ContextualWordCount")
# first5k = sc.textFile(wet_path).take(3)
# firstk, next4k = first5k[:1], first5k[1:]
first5k = sc.textFile(wet_path).take(5000)
firstk, next4k = first5k[:1000], first5k[1000:]
firstk = [path.join(cc_prefix, fn) for fn in firstk]
next4k = [path.join(cc_prefix, fn) for fn in next4k]
firstk_combined = ','.join(firstk)
next4k_combined = ','.join(next4k)

firstk_counts = computeCounts(sc.textFile(firstk_combined))
firstk_counts.filter(lambda x: x[1] >= 30)\
    .coalesce(1)\
    .saveAsTextFile(path.join(my_bucket, 'firstk.txt'))

next4k_counts = computeCounts(sc.textFile(next4k_combined))
first5k_counts = firstk_counts.union(next4k_counts)\
    .reduceByKey(lambda x,y: x + y)
first5k_counts.filter(lambda x: x[1] >= 30)\
    .coalesce(1)\
    .saveAsTextFile(path.join(my_bucket, 'first5k.txt'))
