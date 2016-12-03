from pyspark import SparkContext, StorageLevel
from itertools import combinations
from os import path
from time import time
import pickle

# input_file = 'test'
# input_file = 'hdfs:/ratings/ratings_Video_Games.10k.csv.gz'
input_file = 'hdfs:/ratings/ratings_Video_Games.csv.gz'
# input_file = 'hdfs:/ratings/ratings_Electronics.csv.gz'
# input_file = 'hdfs:/ratings/ratings_Beauty.csv.gz'
# input_file = 'hdfs:/ratings/ratings_Health_and_Personal_Care.csv.gz'
basename = path.basename(input_file)
ts = str(int(time()))
suffix = '_{0}_{1}'.format(basename, ts)
sc = SparkContext(appName='Vj_Apriori' + suffix)

def generateCandidates(basket, k, previous_set):
    candidates = []
    for candidate in combinations(basket, k):
        valid = True
        for subset in combinations(candidate, k-1):
            if len(subset) == 1:
                subset = subset[0]
            if subset not in previous_set:
                #print '{0} failed because of {1}'.format(candidate, subset)
                valid = False
                break
        if valid:
            candidates.append((candidate, 1))

    return candidates

def doesContain(container, items):
    container = set(container)
    return all((item in container for item in items))

def cleanTransaction(transaction, alphabets):
    cleaned_transaction = []
    for item in transaction:
        if item in alphabets:
            cleaned_transaction.append(item)
    return tuple(cleaned_transaction)

if input_file == 'test':
    # TODO: remove this
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
        .map(lambda x: tuple(sorted(x[1])))\
        .persist(StorageLevel.MEMORY_AND_DISK)

    # 1-set frequent items
    frequent_items = user_item_map.map(lambda x: (x[1], 1))\
        .reduceByKey(lambda x,y: x + y)\
        .filter(lambda x: x[1] >= support)

num_transactions = float(transactions.count()) # trigger caching
counts = [[]]
counts.append(frequent_items.collectAsMap())
previous_set = sc.broadcast(counts[-1])
results = []
results.append('1-set count: {0}'.format(len(previous_set.value)))

desired_k = 4
current_k = 2
while current_k <= desired_k:
    candidates = transactions.map(lambda x: \
        generateCandidates(x, current_k, previous_set.value))\
        .persist(StorageLevel.MEMORY_AND_DISK)

    current_set = candidates.flatMap(lambda x: x)\
        .reduceByKey(lambda x,y: x + y)\
        .filter(lambda x: x[1] >= support)

    counts.append(current_set.collectAsMap())
    results.append(
    '{0}-set count: {1}'.format(current_k, len(counts[-1])))

    # dont do filtering if this is the last iteration
    if current_k == desired_k:
        break

    #previous_set.destroy()
    previous_set = sc.broadcast(counts[-1])

    # build alphabet space of still considered items 
    # and remove rest from transactions
    alphabets = set()
    for items in previous_set.value:
        alphabets.update(items)
    alphabets = sc.broadcast(alphabets)

    # filter to transactions that have candidates
    old_transactions = transactions
    transactions = transactions.zip(candidates)\
        .filter(lambda x: len(x[1]) > 0)\
        .map(lambda x: cleanTransaction(x[0], alphabets.value))\
        .repartition(200)\
        .cache()
    #old_transactions.unpersist()
    #candidates.unpersist()
    old_transactions = None
    #alphabets.destroy()
    current_k += 1

rules = []
for items, count in counts[4].iteritems():
    count = float(count)
    for i, item in enumerate(items):
        left = items[:i] + items[i+1:]
        confidence = count / counts[3][left]
        interest = confidence - counts[1][item] / num_transactions
        if confidence >= 0.05 and interest >= 0.02:
            rules.append((left, item, confidence, interest))

results.append(rules)

pickle.dump(results, \
    open('results' + suffix, "wb"))
pickle.dump(counts, \
    open('counts' + suffix, "wb"))

with open('output' + suffix, "wb") as f:
    for line in results[:-1]:
        f.write("%s\n" % line)
    for rule in results[-1]:
        rule = str(rule[0]) + ' --> ' + rule[1]
        f.write("%s\n" % rule)