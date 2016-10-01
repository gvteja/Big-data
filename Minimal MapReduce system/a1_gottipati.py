########################################
## Template Code for Big Data Analytics
## assignment 1, at Stony Brook Univeristy
## Fall 2016

## Vijay Teja Gottipati

## version 1.04
## revision history
##  .01 comments added to top of methods for clarity
##  .02 reorganized mapTask and reduceTask into "system" area
##  .03 updated reduceTask comment  from_reducer
##  .04 updated matrix multiply test

from abc import ABCMeta, abstractmethod
from multiprocessing import Process, Manager
from pprint import pprint
import numpy as np


##########################################################################
##########################################################################
# PART I. MapReduce

class MyMapReduce:#[TODO]
    __metaclass__ = ABCMeta
    
    def __init__(self, data, num_map_tasks=4, num_reduce_tasks=3): #[DONE]
        self.data = data  #the "file": list of all key value pairs
        self.num_map_tasks=num_map_tasks #how many processes to spawn as map tasks
        self.num_reduce_tasks=num_reduce_tasks # " " " as reduce tasks

    ###########################################################   
    #programmer methods (to be overridden by inheriting class)

    @abstractmethod
    def map(self, k, v): #[DONE]
        print "Need to override map"

    
    @abstractmethod
    def reduce(self, k, vs): #[DONE]
        print "Need to override reduce"
        

    ###########################################################
    #System Code: What the map reduce backend handles

    def mapTask(self, data_chunk, map_to_reducer): #[DONE]
        #runs the mappers and assigns each k,v to a reduce task
        for (k, v) in data_chunk:
            #run mappers:
            mapped_kvs = self.map(k, v)
            #assign each kv pair to a reducer task
            for (k, v) in mapped_kvs:
                map_to_reducer.append((self.partitionFunction(k), (k, v)))


    def partitionFunction(self,k): #[TODO]
        #given a key returns the reduce task to send it
        node_number = hash(str(k)) % self.num_reduce_tasks
        return node_number


    def reduceTask(self, kvs, from_reducer): #[TODO]
        #sort all values for each key into a list 
        #[TODO]
        kvs_dict = {}
        for (k, v) in kvs:
            try:
                kvs_dict[k].append(v)
            except KeyError:
                kvs_dict[k] = [v]

        #call reducers on each key paired with a *list* of values
        #and append the result for each key to from_reducer
        #[TODO]
        for (k, vs) in kvs_dict.iteritems():
            from_reducer.append(self.reduce(k, vs))


    def runSystem(self): #[TODO]
        #runs the full map-reduce system processes on mrObject

        #the following two lists are shared by all processes
        #in order to simulate the communication
        #[DONE]
        map_to_reducer = Manager().list() #stores the reducer task assignment and 
                                          #each key-value pair returned from mappers
                                          #in the form: [(reduce_task_num, (k, v)), ...]
        from_reducer = Manager().list() #stores key-value pairs returned from reducers
                                        #in the form [(k, v), ...]
        
        #divide up the data into chunks accord to num_map_tasks, launch a new process
        #for each map task, passing the chunk of data to it. 
        #hint: if chunk contains the data going to a given maptask then the following
        #      starts a process
        #      p = Process(target=self.mapTask, args=(chunk,map_to_reducer))
        #      p.start()  
        #[TODO]
        chunks = [ self.data[i::self.num_map_tasks] for i in xrange(self.num_map_tasks) ]
        mapTasks = []
        for chunk in chunks:
            p = Process(target=self.mapTask, args=(chunk, map_to_reducer))
            p.start()
            mapTasks.append(p)

        #join map task processes back
        #[TODO]
        for p in mapTasks:
            p.join()


        #print output from map tasks 
        #[DONE]
        print "map_to_reducer after map tasks complete:"
        pprint(sorted(list(map_to_reducer)))

        #"send" each key-value pair to its assigned reducer by placing each 
        #into a list of lists, where to_reduce_task[task_num] = [list of kv pairs]
        to_reduce_task = [[] for i in range(self.num_reduce_tasks)] 
        #[TODO]
        for (task_num, kv) in map_to_reducer:
            to_reduce_task[task_num].append(kv)



        #launch the reduce tasks as a new process for each. 
        #[TODO]
        reduceTasks = []
        for reduce_kvs in to_reduce_task:
            p = Process(target=self.reduceTask, args=(reduce_kvs, from_reducer))
            p.start()
            reduceTasks.append(p)


        #join the reduce tasks back
        #[TODO]
        for p in reduceTasks:
            p.join()

        #print output from reducer tasks 
        #[DONE]
        print "map_to_reducer after reduce tasks complete:"
        pprint(sorted(list(from_reducer)))

        #return all key-value pairs:
        #[DONE]
        return from_reducer


##########################################################################
##########################################################################
##Map Reducers:
            
class WordCountMR(MyMapReduce): #[DONE]
    #the mapper and reducer for word count
    def map(self, k, v): #[DONE]
        counts = dict()
        for w in v.split():
            w = w.lower() #makes this case-insensitive
            try:  #try/except KeyError is just a faster way to check if w is in counts:
                counts[w] += 1
            except KeyError:
                counts[w] = 1
        return counts.items()
    
    def reduce(self, k, vs): #[DONE]
        return (k, np.sum(vs))        
    

class MatrixMultMR(MyMapReduce): #[TODO]
    #the mapper and reducer for matrix multiplication
    '''
    map should prbly produce rows for m1 and cols for m2

    reduce should take one row from m1 and cols for m2 and do a dot product. this is the val of i,j in op matrix
    '''
    def __init__(self, data, h1, w1, h2, w2, num_map_tasks=4, num_reduce_tasks=3):
        self.data = data  #the "file": list of all key value pairs
        self.num_map_tasks=num_map_tasks #how many processes to spawn as map tasks
        self.num_reduce_tasks=num_reduce_tasks # " " " as reduce tasks
        # M is h1 x w1 and N is h2 x w2
        self.h1 = h1
        self.w1 = w1
        self.h2 = h2
        self.w2 = w2

    def map(self, k, v):
        kv_dict = {}
        label, r, c = k
        if label.lower() == "m":
            for i in range(self.h2):
                kv_dict[(r, i)] = (label, c, v)
        else:
            for i in range(self.h1):
                kv_dict[(i, c)] = (label, r, v)

        return kv_dict.items()

    
    def reduce(self, k, vs):
        m_row = np.zeros(self.w1)
        n_col = np.zeros(self.w1)
        for (label, pos, v) in vs:
            if label.lower() == "m":
                m_row[pos] = v
            else:
                n_col[pos] = v
        res = np.inner(m_row, n_col)
        return (k, res)    

##########################################################################
##########################################################################
# PART II. Minhashing

def minhash(documents, k=5): #[TODO]
    #returns a minhashed signature for each document
    #documents is a list of strings
    #k is an integer indicating the shingle size

    signatures = None#the signature matrix

    #Shingle Each Document into sets, using k size shingles
    #[TODO]

    #Perform efficient Minhash 
    #[TODO]

    #Print signature matrix and return them 
    #[DONE]
    pprint(signatures)
    return signatures #a minhash signature for each document



##########################################################################
##########################################################################

from scipy.sparse import coo_matrix

def matrixToCoordTuples(label, m): #given a dense matrix, returns ((row, col), value), ...
    cm = coo_matrix(np.array(m))
    return  zip(zip([label]*len(cm.row), cm.row, cm.col), cm.data)

if __name__ == "__main__": #[DONE: Uncomment peices to test]
    ###################
    ##run WordCount:
    data = [(1, "The horse raced past the barn fell"),
            (2, "The complex houses married and single soldiers and their families"),
            (3, "There is nothing either good or bad, but thinking makes it so"),
            (4, "I burn, I pine, I perish"),
            (5, "Come what come may, time and the hour runs through the roughest day"),
            (6, "Be a yardstick of quality."),
            (7, "A horse is the projection of peoples' dreams about themselves - strong, powerful, beautiful"),
            (8, "I believe that at the end of the century the use of words and general educated opinion will have altered so much that one will be able to speak of machines thinking without expecting to be contradicted.")]
    mrObject = WordCountMR(data, 4, 3)
    mrObject.runSystem()
             
    ####################
    ##run MatrixMultiply
    #(uncomment when ready to test)
    data1 = matrixToCoordTuples('m', [[1, 2], [3, 4]]) + matrixToCoordTuples('n', [[1, 2], [3, 4]])
    data2 = matrixToCoordTuples('m', [[1, 2, 3], [4, 5, 6]]) + matrixToCoordTuples('n', [[1, 2], [3, 4], [5, 6]])
    data3 = matrixToCoordTuples('m', np.random.rand(20,5)) + matrixToCoordTuples('n', np.random.rand(5, 40))
    mrObject = MatrixMultMR(data1, 2, 2, 2, 2, 2, 2)
    mrObject.runSystem() 
    mrObject = MatrixMultMR(data2, 2, 3, 3, 2, 2, 2)
    mrObject.runSystem() 
    # mrObject = MatrixMultMR(data3, 6, 6)
    # mrObject.runSystem() 

    ######################
    ## run minhashing:
    # (uncomment when ready to test)
#     documents = ["The horse raced past the barn fell. The complex houses married and single soldiers and their families",
#                  "There is nothing either good or bad, but thinking makes it so. I burn, I pine, I perish. Come what come may, time and the hour runs through the roughest day",
#                  "Be a yardstick of quality. A horse is the projection of peoples' dreams about themselves - strong, powerful, beautiful. I believe that at the end of the century the use of words and general educated opinion will have altered so much that one will be able to speak of machines thinking without expecting to be contradicted."]
#     sigs = minhash(documents, 5)
      
