##########################################################################
## MRSys v1
##
## Implements a basic version of MapReduce intended to run
## on multiple threads of a single system. This implementation
## is intended as an instructional tool for students to
## better understand what a MapReduce system is doing
## in order to better program effective mappers and reducers.
##
## MRSysSim is meant to be inheritted by programs
## using it. See the example "WordCountMR" class for 
## an exaample of how a map reduce programmer would
## use the MRSysSim system by simply defining
## a map and a reduce method. 
##
##
## Original Code written by H. Andrew Schwartz
## for SBU's Big Data Analytics Course 
##
## Student Name:
## Student ID: 


import sys
from abc import ABCMeta, abstractmethod
from multiprocessing import Process, Manager
from pprint import pprint
import numpy as np
from random import random
import time


##########################################################################
##########################################################################
# Map Reduce System Simulator: 

class MRSysSim:
    __metaclass__ = ABCMeta

    def __init__(self, data, num_map_tasks=5, num_reduce_tasks=3, use_combiner = False): 
        self.data = data  #the "file": list of all key value pairs
        self.num_map_tasks=num_map_tasks #how many processes to spawn as map tasks
        self.num_reduce_tasks=num_reduce_tasks # " " " as reduce tasks
        self.use_combiner = use_combiner #whether or not to use a combiner within map task
        
    ###########################################################   
    #programmer methods (to be overridden by inheriting class)

    @abstractmethod
    def map(self, k, v): 
        print("Need to override map")

    
    @abstractmethod
    def reduce(self, k, vs): 
        print("Need to overrirde reduce")
        

    ###########################################################
    #System Code: What the map reduce backend handles

    def mapTask(self, data_chunk, namenode_m2r, combiner=False): 
        #runs the mappers on each record within the data_chunk and assigns each k,v to a reduce task
        mapped_kvs = [] #stored keys and values resulting from a map 
        for (k, v) in data_chunk:
            #run mappers:
            chunk_kvs = self.map(k, v) #the resulting keys and values after running the map task
            mapped_kvs.extend(chunk_kvs) 
			
	#assign each kv pair to a reducer task
        if combiner:
            for_early_reduce = dict()#holds k, vs for running reduce
            #1. Setup value lists for reducers
            for (k, v) in mapped_kvs:
                try: 
                    for_early_reduce[k].append(v)
                except KeyError:
                    for_early_reduce[k] = [v]

            #2. call reduce, appending result to get passed to reduceTasks
            for k, vs in for_early_reduce.items():
                namenode_m2r.append((self.partitionFunction(k), self.reduce(k, vs)))
            
        else:
            for (k, v) in mapped_kvs:
                namenode_m2r.append((self.partitionFunction(k), (k, v)))


    def partitionFunction(self,k): 
        #given a key returns the reduce task to send it
        node_number = np.sum([ord(c) for c in str(k)]) % self.num_reduce_tasks
        return node_number


    def reduceTask(self, kvs, namenode_fromR): 
        #Sort such that all values for a given key are in a 
        #list for that key 
        vsPerK = dict()
        for (k, v) in kvs:
            try:
                vsPerK[k].append(v)
            except KeyError:
                vsPerK[k] = [v]
        
        #Call self.reduce(k, vs) for each each key, providing 
        #its list of values and append the results (if they exist) 
        #to the list variable "namenode_fromR" 
        for k, vs in vsPerK.items():
            if vs:
                fromR = self.reduce(k, vs)
                if fromR:#skip if reducer returns nothing (no data to pass along)
                    namenode_fromR.append(fromR)
        pass

    def runSystem(self): 
        #runs the full map-reduce system processes on mrObject

        #STEP-1
        #The following two lists are shared by all processes
        #in order to simulate the communication
        namenode_m2r = Manager().list() #stores the reducer task assignment and 
                                          #each key-value pair returned from mappers
                                          #in the form: [(reduce_task_num, (k, v)), ...]
                                          #[COMBINER: when enabled this might hold]
        namenode_fromR = Manager().list() #stores key-value pairs returned from reducers
                                          #in the form [(k, v), ...]

        #STEP-2
        #divide up the data into chunks accord to num_map_tasks, launch a new process
        #for each map task, passing the chunk of data to it. 
        #the following starts a process
        #      p = Process(target=self.mapTask, args=(chunk,namenode_m2r))
        #      p.start()  
        processes = []
        chunkSize = int(np.ceil(len(self.data) / int(self.num_map_tasks)))

        chunkStart = 0
        while chunkStart < len(self.data):
            chunkEnd = min(chunkStart+chunkSize, len(self.data))
            chunk = self.data[chunkStart:chunkEnd]
            #print(" starting map task on ", chunk) #debug
            processes.append(Process(target=self.mapTask, args=(chunk,namenode_m2r,self.use_combiner)))
            processes[-1].start()
            chunkStart = chunkEnd

        #STEP-3
        #join map task processes back
        for p in processes:
            p.join()
		        #print output from map tasks 
        #print("namenode_m2r after map tasks complete:")
        #pprint(sorted(list(namenode_m2r)))

        #STEP-4
        #"send" each key-value pair to its assigned reducer by placing each 
        #into a list of lists, where to_reduce_task[task_num] = [list of kv pairs]
        #[[TODO:PartII.A]]
        to_reduce_task = [[] for i in range(self.num_reduce_tasks)]
        for (rtask_num, kv) in namenode_m2r:
            to_reduce_task[rtask_num].append(kv)
        
        #STEP-5
        #launch the reduce tasks as a new process for each. 
        processes = []
        for kvs in to_reduce_task:
            processes.append(Process(target=self.reduceTask, args=(kvs, namenode_fromR)))
            processes[-1].start()

        #STEP-6
        #join the reduce tasks back
        for p in processes:
            p.join()
        #print output from reducer tasks 
        print("namenode_fromR after reduce tasks complete:")
        pprint(sorted(list(namenode_fromR)))

        #return all key-value pairs:
        return namenode_fromR


##########################################################################
##########################################################################


class meanMRSys(MRSysSim):
    #[[TODO:PartII.C]] Create the map and reduce functions to return the mean r, g, and, b
    
    def map(self, k, v):
        r, g, b = v
        intermediate_key_values = {
            'red': (r, 1),
            'green': (g, 1),
            'blue': (b, 1)
        }
        return [(color, intermediate_key_values[color]) for color in intermediate_key_values]
    
    def reduce(self, k, vs):
        # Compute the global sum and count for the color using numpy
        vs_array = np.array(vs)
        global_sum = np.sum(vs_array[:,0])
        global_count = np.sum(vs_array[:,1])
        
        # Compute the mean and return it as a tuple
        mean = global_sum / global_count if global_count > 0 else 0
        return (k, mean)


        # convert the means array back into a list of (color, mean) tuples
       
if __name__ == "__main__": #[Uncomment peices to test]
    
    print("\n\nTESTING YOUR CODE\n")
    #start_time=time.time()
    ##Mean
    print("\n\n*************************\n Mean \n*************************\n")
    filename = sys.argv[1]
    data = []
    with open(filename, 'r') as infile:
        data = [eval(i.strip()) for i in infile.readlines()]
    data = list(zip(range(len(data)), data))
        
    #print("\nExample of input data: ", data[:10])
    mrObject = meanMRSys(data, 4, 3)
    mrObject.runSystem()
    #print("--- %s seconds ---" % (time.time() - start_time))

    