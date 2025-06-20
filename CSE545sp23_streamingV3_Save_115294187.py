##########################################################################
## CSE545sp23_streamingV3_Save_115294187.py
## version 3:
##   -- prints stream outputs in more helpful format
##   -- adds element=line.strip() in task1B
##   -- enables 160k booleans for part 1b
## 
## Template code for assignment 1 part 1. 
## Do not edit anywhere except blocks where a #[TODO]# appears
##
## Student Name: Rucha Save
## Student ID: 115294187


import sys
from pprint import pprint
from random import random
from collections import deque
from sys import getsizeof
try:
    import resource
except:
    pass
from math import log, log2 #natural log
import numpy as np
import mmh3 #hashing library

##########################################################################
##########################################################################
# Methods: implement the methods of the assignment below.  
#
# Each method gets 1 100 element array for holding ints of floats. 
# This array is called memory1a, memory1b, or memory1c
# You may not store anything else outside the scope of the method.
# "current memory size" printed by main should not exceed 8,000.

MEMORY_SIZE = 1000 #do not edit
#this is the only memory you get for 1a; a deque functions just like an array
#it is of size 1,000
memory1a =  deque([None] * MEMORY_SIZE, maxlen=MEMORY_SIZE) #do not edit

def task1A_meanRGBsStream(element, returnResult = True):
    #[TODO]#
    #procss the element you may only use memory, storing at most 1000
    element = element.lstrip('(')
    element = element.rstrip(')')
    r, g, b = map(int, element.split(','))
    memory1a.append((r, g, b))
    if returnResult: #when the stream is requesting the current result
        #[TODO]#
        #any additional processing to return the result at this point
        #BEGIN[Google][https://note.nkmk.me/en/python-numpy-dtype-astype/]"How to Cast Np array to specific data Type"
        sum_r = sum([np.float64(x[0]) for x in memory1a if x is not None])
        sum_g = sum([np.float64(x[1]) for x in memory1a if x is not None])
        sum_b = sum([np.float64(x[2]) for x in memory1a if x is not None])
        count = len([x for x in memory1a if x is not None])
        if count > 0:
            valuemean_r = sum_r / np.float64(count)
            valuemean_g = sum_g / np.float64(count)
            valuemean_b = sum_b / np.float64(count)
            return (valuemean_r, valuemean_g, valuemean_b)
        else:
            return (np.float64(0), np.float64(0), np.float64(0))
        #END[Google]
    else: #no need to return a result
        pass

#ADDED FOR V3:
MEMORY_SIZE = 160000 #do not edit
#this is the only memory you get for 1B; a deque functions just like an array
#it is of size 60,000 -- you should only store booleans or {0, 1}
memory1b =  deque([0] * MEMORY_SIZE, maxlen=MEMORY_SIZE) #do not edit

##You may add custom functions here. Storage of anything besides hash functions is not permitted
#BEGIN[Google][https://www.enjoyalgorithms.com/blog/bloom-filter]"Bloomfilter Implementation"
def calculate_hash_rgb(rgb, tseed):
    #rgb_byte = bytes(str(rgb),'UTF-8')
    calcluate_hash = mmh3.hash( rgb, tseed)
    return calcluate_hash % MEMORY_SIZE

#saving only hash values 
saved_hash = []
for i in range(30):
    tseed = i
    saved_hash.append(lambda x, s=tseed: calculate_hash_rgb(x, s))

def task1B_bloomSetup(elements_in_set):
    #[TODO]#
    #setup the bloom filter memory to be able to filter streaming elements
    # Extract colors from first 2000 elements
    for elementi in elements_in_set:
        for hash_function in saved_hash:
            memory1b[hash_function(elementi)%MEMORY_SIZE] = 1

    
def task1B_bloomStream(element):
    #[TODO]#
    #procss the element, using at most the 1000 dimensions of memory
    #return True if the element is determined to be in the bloom filter set
    for hash_function_index in saved_hash:
                    if  memory1b[hash_function_index(element)%MEMORY_SIZE] == 1:
                         return True
    return False
#END[Google]
##########################################################################
##########################################################################
# MAIN: the code below setups up the stream and calls your methods
# Printouts of the results returned will be done every so often
# DO NOT EDIT BELOW

def getMemorySize(l): #returns sum of all element sizes
    return sum([getsizeof(e) for e in l])+getsizeof(l)

if __name__ == "__main__": #[Uncomment peices to test]
    
    print("\n\nTESTING YOUR CODE\n")
    
    ###################
    ## The main stream loop: 
    print("\n\n*************************\n Beginning stream input \n*************************\n")
    filename = sys.argv[1]#the data file to read into a stream
    printLines = frozenset([5**i for i in range(1, 20)]) #stores lines to print
    peakMem = 0 #tracks peak memory usage
    all = []#DEBUG
    
    with open(filename, 'r') as infile:
        i = 0#keeps track of lines read
        for line in infile:
        
            #remove \n and convert to int
            element = line.strip()
            #all.append(element)#DEBUG
            i += 1
            
            #call tasks         
            if i in printLines: #print status at this point: 
                result1a = task1A_meanRGBsStream(element, returnResult=True)
                print(" Result at stream element # %d:" % i)
                print("   1A:   means: %s" % str(["%.2f" % float(m) for m in result1a]))
                print(" [current memory1a size: %d]\n" % \
                    (getMemorySize(memory1a))) #<- change to memory1a in V3
                
            else: #just pass for stream processing
                result1a = task1A_meanRGBsStream(element, False)
                
            try:
                memUsage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                if memUsage > peakMem: peakMem = memUsage
            except:
                pass
        
    print("\n*******************************\n    Stream mean Terminated \n*******************************")
    if peakMem > 0:
        print("(peak memory usage was: ", peakMem, ")")

    peakMem = 0 #tracks peak memory usage
    bloomSetSize = min(2000,0.1*i) #set the bloom filter set size (smaller for trial data)
    with open(filename, 'r') as infile:
        i = 0#keeps track of lines read
        bloomSet = []
        for line in infile:
            bloomSet.append(line.strip())
            #all.append(element)#DEBUG
            i += 1
            if i > bloomSetSize:
                break
        #setup bloom
        task1B_bloomSetup(bloomSet)        

        print("\n*******************************\n   Bloom Setup, Streaming: \n*******************************")
        
        for line in infile:
            #remove \n and convert to int
            element = line.strip()
            i += 1

            #call tasks
            result1b = task1B_bloomStream(element)
            if result1b: #print status at this point:                
                print(" Result at stream element # %d:" % i)
                print("   1B: element: %s" % str(element))
                print("   1B:   bloom: %s" % str(result1b))
                print(" [current memory size: %d]\n" % \
                    (getMemorySize(memory1b))) #<- change to memory1b in V3
            try:
                memUsage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                if memUsage > peakMem: peakMem = memUsage
            except:
                pass
        
    print("\n*******************************\n   Stream bloom Terminated \n*******************************")
    if peakMem > 0:
        print("(peak memory usage was: ", peakMem, ")")

        