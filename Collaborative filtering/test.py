from pyspark import SparkContext, StorageLevel
from itertools import combinations
from os import path
from time import time
import pickle

# input_file = 'test'
input_file = 'Downloads/Data/ratings_Video_Games.csv.gz'
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

def computeScore(tup, norms, interested_norm):
    item, (dot, _) = tup
    norm = norms[item]
    # Can remove this check if we filter out things with 0 norm
    if not norm:
        return (x[0], 0)
    return (item, dot/(norm * interested_norm))

def computeProductIntersection(tup, interested_row):
    item, (user, rating) = tup
    try:
        product = interested_row[user] * rating
    except:
        # user not in interested row
        # no intersection and product is 0
        return []
    return [(item, (product, 1))]


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

users = user_item_map.keys().distinct().collect()

item_user_map = user_item_map.map(lambda x: ((x[1][0]), (x[0], x[1][1])))

# 105370 number of tuple in item_user_map at this stage

# TODO: see if we can force a paritioning by item id/key to optimize next ops

items = item_user_map.keys().distinct().collect()

means = item_user_map.map(lambda x: (x[0], (x[1][1], 1.0)))\
    .reduceByKey(lambda x,y: ((x[0] + y[0]), (x[1] + y[1])))\
    .map(lambda x: (x[0], x[1][0]/x[1][1]))

# normalize item user ratings
item_user_map = item_user_map.join(means)\
    .map(lambda x: (x[0], (x[1][0][0], x[1][0][1] - x[1][1])))

# TODO: filter out users/items with zero variance
# Maybe can reduce by key and take an or operation

norms = item_user_map.map(lambda x: (x[0], x[1][1] ** 2))\
    .reduceByKey(lambda x,y: x + y)\
    .map(lambda x: (x[0], x[1] ** 0.5))
no_variance_items = norms.filter(lambda x: x[1] == 0)
    .keys()
norms = sc.broadcast(norms.collectAsMap())

# maybe throw rows with 0 norms. they are zero variance. or rows with < some small epsilon

interested_row = item_user_map.lookup(interested_item)
interested_row = {u:r for (u, r) in interested_row}
interested_row = sc.broadcast(interested_row)

interested_norm = norms.value[interested_item]

# compute cosine scores for and get all valid neighbours
# valid implies having >= 2 users in common 
# and having cosine sim > 0
scores = item_user_map.flatMap(\
    lambda x: computeProductIntersection(x, interested_row.value))\
    .reduceByKey(lambda x,y: ((x[0] + y[0]), (x[1] + y[1])))\
    .filter(lambda x: x[1][1] >= 2)\
    .map(lambda x: computeScore(x, norms.value, interested_norm))\
    .filter(lambda x: x[1] > 0)\


# With a target row, skip columns that do not have at least 2 neighbors

# can we throw our users also with no var? would it not make a diff?

# computer interested users rdd
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




is cogroup same as doing a groupByKey and then joining on those?

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