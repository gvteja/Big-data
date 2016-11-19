from pyspark import SparkContext

sc = SparkContext(appName="Apriori_Vijay")

def extractItemUser(line):
    user, item, _, _ = line.split(',')
    return (item, user)

rdd = sc.textFile('/Users/bobby/Downloads/ratings_Beauty.csv', use_unicode=False)
#rdd = sc.parallelize(rdd, 100)
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

#item_user_map.map(lambda x: (x[0], 1)).distinct().count()

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

# generate cand set from freq[k-1]
# prune cand set by using subset property
# generate count for each remaining cand
# filter cand by support
# freq[k] = filtered cand

# todo:
# cand gen
# prune
# count cand
