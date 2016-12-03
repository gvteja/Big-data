from pyspark import SparkContext, StorageLevel
from itertools import combinations
from os import path
from time import time
import pickle

# input_file = 'test'
input_file = 'Downloads/Data/ratings_Video_Games.10k.csv.gz'
# #input_file = 'hdfs:/ratings/ratings_Video_Games.10k.csv.gz'
input_file = 'hdfs:/ratings/ratings_Video_Games.csv.gz'
# input_file = 'hdfs:/ratings/ratings_Electronics.csv.gz'
# input_file = 'hdfs:/ratings/ratings_Beauty.csv.gz'
# input_file = 'hdfs:/ratings/ratings_Health_and_Personal_Care.csv.gz'
interested_item = 'B000035Y4P'
basename = path.basename(input_file)
ts = str(int(time()))
suffix = '_{0}_{1}'.format(basename, ts)
sc = SparkContext(appName='Vj_CF' + suffix)

def extractAsTuple(line):
    user, item, rating, t = [x.strip() for x in line.split(',')]
    return ((item, user), (float(rating), int(t)))

def computeScore(x, norms, interested_norm):
    norm = norms[x[0]]
    if not norm:
        return (x[0], 0)
    return (x[0], x[1]/(norm * interested_norm))

rdd = sc.textFile(input_file, use_unicode=False)
item_user_map = rdd.map(extractAsTuple)\
    .reduceByKey(lambda r1,r2: r1 if r1[1] > r2[1] else r2)\
    .map(lambda x: ((x[0][0]), (x[0][1], x[1][0])))

items_to_remove = item_user_map.map(lambda x: (x[0], 1))\
    .reduceByKey(lambda x,y: x + y)\
    .filter(lambda x: x[1] < 25)
item_user_map = item_user_map.subtractByKey(items_to_remove)

user_item_map = item_user_map.map(lambda x: ((x[1][0]), (x[0], x[1][1])))
users_to_remove = user_item_map.map(lambda x: (x[0], 1))\
    .reduceByKey(lambda x,y: x + y)\
    .filter(lambda x: x[1] < 10)
user_item_map = user_item_map.subtractByKey(users_to_remove)

users = user_item_map.keys().collect()

item_user_map = user_item_map.map(lambda x: ((x[1][0]), (x[0], x[1][1])))

# 105370 number of tuple in item_user_map at this stage

# TODO: see if we can force a paritioning by item id/key to optimize next ops

items = item_user_map.keys().collect()

means = item_user_map.map(lambda x: (x[0], (x[1][1], 1.0)))\
    .reduceByKey(lambda x,y: ((x[0] + y[0]), (x[1] + y[1])))\
    .map(lambda x: (x[0], x[1][0]/x[1][1]))

# normalize item user ratings
item_user_map = item_user_map.join(means)\
    .map(lambda x: (x[0], (x[1][0][0], x[1][0][1] - x[1][1])))

norms = item_user_map.map(lambda x: (x[0], x[1][1] ** 2))\
    .reduceByKey(lambda x,y: x + y)\
    .map(lambda x: (x[0], x[1] ** 0.5))
norms = sc.broadcast(norms.collectAsMap())

interested_row = item_user_map.lookup(interested_item)
interested_row = {u:r for (u, r) in interested_row}
interested_row = sc.broadcast(interested_row)

interested_norm = norms.value[interested_item]

# compute cosine scores with all items
scores = item_user_map.map(\
    lambda x: (x[0], x[1][1] * interested_row.value.get(x[1][0], 0)))\
    .reduceByKey(lambda x,y: x + y)\
    .map(lambda x: computeScore(x, norms.value, interested_norm))\


# verify self cosine score is 1

for user in users:
    if user in interested_row:
        continue
    # we need to fill the missing val for this user
    # find all rating of this user
    # can do a lookup on user-item
    # find the top 50 of these items based on their score
    # compute a weighted avg
    # can we parallelize this
    # maybe create a users rdd. do subtractbykey of interested row rdd
    # join these interested users rdd with user-item rdd




    .top(50, key=lambda x: x[1])

check division by error

get the rdd for the item that we are interested in
maybe bcast this item row to everyone
now do a map on the (item, (user, rating)) rdd
    we need to compute a cosine sim (can we optimize to do just a dot product by normalizing?)
    so whenever the user matches with a user who rated, emit a value, else nothing?

for all items -> have set of items

for normalizing, we need to find the mean
maybe we can do 2 reduceByKey, one to find the mean and then a map to subtract mean
then we do another reduceByKey taking the squares and summing
in the end, we map and take square root of the sum of squares

is cogroup same as doing a groupByKey and then joining on those?



transactions = user_item_map.map(lambda x: (x[0], [x[1]]))\
    .reduceByKey(lambda x,y: x + y)\
    .map(lambda x: tuple(sorted(x[1])))\
    .persist(StorageLevel.MEMORY_AND_DISK)

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
    for line in results[-1]:
        f.write("%s\n" % line)
    for rule in rules:
        rule = rule[0] + ' --> ' + rule[1]
        f.write("%s\n" % rule)