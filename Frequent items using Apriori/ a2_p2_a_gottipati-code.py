from pyspark import SparkContext, StorageLevel

sc = SparkContext(appName="Apriori_Vijay")

def extractItemUser(line):
    user, item, _, _ = line.split(',')
    return (item, user)

rdd = sc.textFile('hdfs:/ratings/ratings_Video_Games.10k.csv.gz', use_unicode=False)
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
    .map(lambda x: set(x[1]))\
    .persist(StorageLevel.MEMORY_AND_DISK)

def generateCandidate(tup):
    (s1, _),  (s2, _) = tup
    s1, s2 = set(s1), set(s2)
    n = len(s1)
    union = s1.union(s2)
    if len(union) != n + 1:
        # cannot combine the two sets
        return []
    [e2] = union - s1
    [e1] = union - s2
    if e1 > e2:
        # cartesian product generates duplicates
        # i.e. (s1, s2) and (s2, s1)
        # so skipping the pair where s1 has the bigger eleme in the union
        return []
    return tuple(sorted(union))

def generatePruneDependencies(tup):
    # assuming the tuple here is already sorted
    l = list(s)
    n = len(l)
    dependencies = []
    for i in range(n):
        dep = l[:i] + l[i+1:]
        dependencies.append((tuple(dep), tup))

    return dependencies

def isSetInTransaction(tup):
    s, t = tup
    in_t = all((item in t for item in s))
    val = 1 if in_t else 0
    return (s, val)

set1 = user_item_map.map(lambda x: (x[1], 1))\
    .reduceByKey(lambda x,y: x + y)\
    .filter(lambda x: x[1] >= 10)\
    .map(lambda x: ((x[0],), None))
prune_deps = set1.cartesian(set1)\
    .map(generateCandidate)\
    .filter(lambda x: len(x) > 0)\
    .flatMap(generatePruneDependencies)
pruned_set2 = set1.leftOuterJoin(prune_deps)\
    .map(lambda x: (x[0], x[1][1]))\
    .filter(lambda x: x[1])\
    .map(lambda x: (x[1], 1))\
    .reduceByKey(lambda x,y: x + y)\
    .filter(lambda x: x[1] == 2)\
    .map(lambda x: x[0])

set2 = pruned_set2.cartesian(transactions)\
    .map(isSetInTransaction)\
    .reduceByKey(lambda x,y: x + y)\
    .filter(lambda x: x[1] >= 10)\
    .map(lambda x: ((x[0],), None))

# For beauty product ratings
# In [6]: user_item_map.count()
# Out[6]: 291167

# In [7]: items_to_remove.count()
# Out[7]: 212336

# In [8]: users_to_remove.count()
# Out[8]: 938854

# In [9]: rdd.count()
# Out[9]: 2023070

# In [10]: item_user_map.count()
# Out[10]: 1514617

# In [16]: user_item_map.map(lambda x: (x[0], 1)).distinct().count()
# Out[16]: 33353

# In [18]: user_item_map.map(lambda x: (x[1], 1)).distinct().count()
# Out[18]: 33327

# generate cand set from freq[k-1]
# prune cand set by using subset property
# generate count for each remaining cand
# filter cand by support
# freq[k] = filtered cand

# one line of thought:
# do a group by on the user_item_map. we get transactions, kind of
# maybe map value and create a set out of it

# cand gen:
#     do self join of k-1 sets
#     merge both sets. new set should increase by just one
#     but we will have duplicate, 2 for each set
#     eg: (1,2) with (2,3) and (2,3) with (1,2) => (1,2,3)

# prune:
#     filter fn on cand set which check that all k subsets are in the old k-1 frequent set
# so basically each cand has k dependencies
# so for every cand of size k, gen k dep with dep as key and val as cand
# then do a left outer join or something of the k-1 frequent set with the dep set
# then swap key-val pair and reduce by key with cand set now
# use tuple(sorted(set)) to generate hashable key for all these
# now after agg prune everything with less than k-1
#     maybe use a all(generator for set check)

# and then for count do a reduce/agg
#     but we need to pass in a set of items to all reducers
#         for each cand, we need a bcase for its set of elems
#     can we use a broadcast var? can we use same var name and over-write it?
#     no right? bcast var cannot be written to i guess

# maybe do a cartesian bw cand and transactions
# then map and return if key/cand is present in transaction
# then reduceByKey to get support
