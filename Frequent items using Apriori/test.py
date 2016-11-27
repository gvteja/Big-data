from pyspark import SparkContext, StorageLevel
from os import path
from time import time
import pickle

support = 2
aggressive_pruning = False
#input_file = 'test'
input_file = 'hdfs:/ratings/ratings_Video_Games.10k.csv.gz'
basename = path.basename(input_file)
ts = str(time())
results = []
suffix = '_{0}_{1}'.format(basename, ts)
sc = SparkContext(appName='Vj_Apriori' + suffix)

def generateCandidate(tup):
    (common_items, (i1, i2)) = tup
    if i1 < i2:
        combined_items = common_items + (i1, i2)
        return [combined_items]
    return []

def doesContain(container, items):
    container = set(container)
    return all((item in container for item in items))

if input_file == "test":
    test_data = '''1,2,5
    2,4
    2,3
    1,2,3,4,5
    1,3
    2,3
    1,3
    1,2,3,5
    1,2,3'''
    test = sc.parallelize(test_data.split('\n'))\
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

previous_set = frequent_items.keys()
results.append(
        '1-set count: {0}'.format(previous_set.count()))

desired_k = 4
current_k = 2
while current_k <= desired_k:
    if current_k == 2:
        # pruned_candidates are just unique pairs of
        # 1-set frequent items
        candidates = previous_set.cartesian(previous_set)\
            .filter(lambda x: x[0] < x[1])
    else:
        previous_set = previous_set.map(lambda x: (x[:-1], x[-1]))
        candidates = previous_set.join(previous_set)\
            .flatMap(generateCandidate)

    if aggressive_pruning: 
        candidates = candidates.cartesian(previous_set)\
            .filter(lambda x: doesContain(x[0], x[1]))\
            .map(lambda x: (x[0], 1))\
            .reduceByKey(lambda x,y: x + y)\
            .filter(lambda x: x[1] == current_k)\
            .keys()
    
    current_set = candidates.cartesian(transactions)\
        .filter(lambda x: doesContain(x[1], x[0]))\
        .map(lambda x: (x[0], 1))\
        .reduceByKey(lambda x,y: x + y)\
        .filter(lambda x: x[1] >= support)\
        .keys()

    results.append(
        '{0}-set count: {1}'.format(current_k, current_set.count()))
    previous_set = current_set
    current_k += 1

results.append(current_set.collect())

pickle.dump(results, \
    open('results' + suffix, "wb"))