from pyspark import SparkContext, StorageLevel
from itertools import combinations
from os import path
from time import time
import pickle

aggressive_pruning = False
#input_file = 'test'
input_file = 'hdfs:/ratings/ratings_Video_Games.csv.gz'
basename = path.basename(input_file)
ts = str(int(time()))
results = []
suffix = '_{0}_{1}'.format(basename, ts)
sc = SparkContext(appName='Vj_Apriori' + suffix)

def generateCandidates(basket, k, previous_set):
    candidates = []
    for candidate in combinations(basket, k):
        valid = True
        for subset in combinations(candidate, k-1):
            if len(subset) == 1:
                subset = subset[0]
            else:
                subset = tuple(sorted(subset))
            if subset not in previous_set:
                #print '{0} failed because of {1}'.format(candidate, subset)
                valid = False
                break
        if valid:
            candidate = tuple(sorted(candidate))
            candidates.append((candidate, 1))

    return candidates

def doesContain(container, items):
    container = set(container)
    return all((item in container for item in items))

if input_file == 'test':
    test_data = '''1,2,5
    2,4
    2,3
    1,2,3,4,5
    1,3
    2,3
    1,3
    1,2,3,5
    1,2,3'''
    test = sc.parallelize([x.strip() for x in test_data.split('\n')])\
        .map(lambda x: tuple(x.split(',')))
    transactions = test
    support = 2

    # 1-set frequent items
    frequent_items = test.flatMap(lambda x: [(i, 1) for i in x])\
        .reduceByKey(lambda x,y: x + y)\
        .filter(lambda x: x[1] >= support)
else:
    support = 10

    def extractItemUser(line):
        user, item, _, _ = [x.strip() for x in line.split(',')]
        return (item, user)

    rdd = sc.textFile(input_file, use_unicode=False)
    #rdd = sc.textFile('/Users/bobby/Downloads/ratings_Beauty.csv', use_unicode=False)
    item_user_map = rdd.map(extractItemUser).distinct()
    items_to_remove = item_user_map.map(lambda x: (x[0], 1))\
        .reduceByKey(lambda x,y: x + y)\
        .filter(lambda x: x[1] < 10)
    item_user_map = item_user_map.subtractByKey(items_to_remove)

    user_item_map = item_user_map.map(lambda x: (x[1], x[0]))
    users_to_remove = user_item_map.map(lambda x: (x[0], 1))\
        .reduceByKey(lambda x,y: x + y)\
        .filter(lambda x: x[1] < 5)
    user_item_map = user_item_map.subtractByKey(users_to_remove)

    transactions = user_item_map.map(lambda x: (x[0], [x[1]]))\
        .reduceByKey(lambda x,y: x + y)\
        .map(lambda x: tuple(x[1]))\
        .persist(StorageLevel.MEMORY_AND_DISK)
    transactions.count() # to trigger caching

    # 1-set frequent items
    frequent_items = user_item_map.map(lambda x: (x[1], 1))\
        .reduceByKey(lambda x,y: x + y)\
        .filter(lambda x: x[1] >= support)

counts_1set = frequent_items.collectAsMap()
previous_set = sc.broadcast(set(counts_1set.keys()))

results.append('1-set count: {0}'.format(len(previous_set.value)))

desired_k = 4
current_k = 2
while current_k <= desired_k:
    current_set = transactions.flatMap(lambda x: \
            generateCandidates(x, current_k, previous_set.value))\
        .reduceByKey(lambda x,y: x + y)\
        .filter(lambda x: x[1] >= support)

    previous_set = sc.broadcast(set(current_set.keys().collect()))
    results.append(
        '{0}-set count: {1}'.format(current_k, len(previous_set.value)))
    current_k += 1

counts_4set = current_set.collectAsMap()
#results.append(current_set.collect())

pickle.dump(results, \
    open('results' + suffix, "wb"))