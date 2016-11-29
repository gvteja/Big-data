from pyspark import SparkContext, StorageLevel

sc = SparkContext(appName="Apriori_Vijay")

support = 10
aggressive_pruning = False

def extractItemUser(line):
    user, item, _, _ = [x.strip() for x in line.split(',')]
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
    .map(lambda x: tuple(x[1]))\
    .persist(StorageLevel.MEMORY_AND_DISK)
transactions.count() # to trigger caching

def generateCandidate(tup):
    (common_items, (i1, i2)) = tup
    if i1 < i2:
        combined_items = common_items + (i1, i2)
        return [combined_items]
    return []

def doesContain(container, items):
    container = set(container)
    return all((item in container for item in items))

# 1-set frequent items
frequent_items = user_item_map.map(lambda x: (x[1], 1))\
    .reduceByKey(lambda x,y: x + y)\
    .filter(lambda x: x[1] >= support)

previous_set = frequent_items.keys()

desired_k = 2
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

    print '{0}-set count: {1}'.format(current_k, current_set.count())
    previous_set = current_set
    current_k += 1


test_data1 = '''1,2,5
2,4
2,3
1,2,4
1,3
2,3
1,3
1,2,3,5
1,2,3'''
test_data2 = '''1,2,5
2,4
2,3
1,2,3,4,5
1,3
2,3
1,3
1,2,3,5
1,2,3'''
test = sc.parallelize(test_data2.split('\n'))\
    .map(lambda x: tuple(x.split(',')))
transactions = test
support = 2

# 1-set frequent items
frequent_items = test.flatMap(lambda x: [(i, 1) for i in x])\
    .reduceByKey(lambda x,y: x + y)\
    .filter(lambda x: x[1] >= support)

previous_set = frequent_items.keys()

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


    previous_set = current_set
    current_k += 1
    print current_set.collect()


# todo: take care of persiting prev k-set and transactions
# are transactions as intensive as the other ones?
# check stages to understand
# another opti - make transactions as bcast var
# then to count, map on cand set
#   in the map function, count all the times the current set is in transactions
# or maybe bcast the candidates because they might be smaller than transactions - not true. can gen n^2 (relative to freq set 1) candidates
# also explore mapPartitions
# also think about optimizing pruning. generating dependencies are creating too many tuples
# explore dataframes api too maybe

replace leftOuterJoin with join?
duplaicte transaction on all nodes to disk?
try reducing transactions by count
also do join on previous set with transacion. and filter out all transactions without atleast one frequent itemset
also maybe do one more filtering on the transaction value. filter out all items which are not part of any frequent set. but tis is just memory saving in value. might still be useful
try foreach
flatMap on count and dontreturn not found tuple
instead of generating dep, directly cartesian previous with candidates
    then count if prev set in cand. then reduce by candidates. we should have k-1 for all pruned candidates
do a join on previous set for cand gen instead of cartesian
change partition to sc.defaultParallelism * 2 or 3
add check to do pruning only for 3-sets and above
add check to coninue genrating only if count of prev set > 0
make make 1-set frequent items into a broadcast var
can we make all candidates counts as bcast too?
    make we can do this:
    make all l-sets as bcast var
    do a flatMap on transactions, in the map fn, generate all k-set items where all its k-1 subsets are in l-set. then do a reduceByKey and then filter
check if something is broadcast join
refactor transactions to directly add tuple instead of adding as list and then making tuples. is it really better though? its like adding immutable strings, since tuples are immutable but lists are. so perf wise might not be really better, but codewise it is, and maybe it avoids another pass on the data. should check if chaining is done as pipeline or as multiple passes

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
