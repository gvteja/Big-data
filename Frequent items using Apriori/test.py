from pyspark import SparkContext, StorageLevel

sc = SparkContext(appName="Apriori_test_Vijay")


def generateCandidate(tup):
    (s1, _),  (s2, _) = tup
    s1, s2 = set(s1), set(s2)
    try:
        n = len(s1)
        union = s1.union(s2)
    except:
        print tup, s1, s2
    if len(union) != n + 1:
        # cannot combine the two sets
        return []
    [e2] = union - s1
    [e1] = union - s2
    if e1 > e2:
        # cartesian product generates duplicates
        # i.e. (s1, s2) and (s2, s1)
        # so skipping the pair where s1 has the bigger element in the union
        return []
    return tuple(sorted(union))

def generateCandidate2(tup):
    (common_items, (i1, i2)) = tup
    if not i1 or not i2:
        # generating from 1-set
        common_items = [] 
    if i1 < i2:
        new_items = [i1, i2]
    return tuple(sorted(union))

def generatePruneDependencies(tup):
    # assuming the tuple here is already sorted
    l = list(tup)
    n = len(l)
    dependencies = []
    for i in range(n):
        dep = l[:i] + l[i+1:]
        dependencies.append((tuple(dep), tup))

    return dependencies

def isSetInTransaction(tup):
    s, t = tup
    t = set(t)
    in_t = all((item in t for item in s))
    val = 1 if in_t else 0
    return (s, val)


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

previous_set = test.flatMap(lambda x: [(i, 1) for i in x])\
    .reduceByKey(lambda x,y: x + y)\
    .filter(lambda x: x[1] >= support)\
    .map(lambda x: ((x[0],), None))

desired_k = 3
current_k = 2
while current_k <= desired_k:
    prune_deps = previous_set.cartesian(previous_set)\
        .map(generateCandidate)\
        .filter(lambda x: len(x) > 0)\
        .distinct()\
        .flatMap(generatePruneDependencies)
    pruned_candidates = previous_set.join(prune_deps)\
        .map(lambda x: (x[1][1], 1))\
        .reduceByKey(lambda x,y: x + y)\
        .filter(lambda x: x[1] == current_k)\
        .keys()

    current_set = pruned_candidates.cartesian(transactions)\
        .map(isSetInTransaction)\
        .reduceByKey(lambda x,y: x + y)\
        .filter(lambda x: x[1] >= support)\
        .map(lambda x: (x[0], None))

    previous_set = current_set
    current_k += 1
    print current_set.collect()