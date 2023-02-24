import sys
from pprint import pprint
from random import random
from collections import deque
from sys import getsizeof
import math
try:
    import resource
except:
    pass
from math import log, log2 #natural log
import numpy as np
import mmh3 #hashing library

MEMORY_SIZE = 1000
memory =  deque([None] * MEMORY_SIZE, maxlen=MEMORY_SIZE)
import sys
import numpy as np

import sys
import numpy as np
import random

def task1A_meanRGBsStream(element,returnResult = True):
    #[TODO]#
    #procss the element you may only use memory, storing at most 1000 
    element = element.lstrip('(')
    element = element.rstrip(')')
    r, g, b = map(int, element.split(','))
    memory.append((r, g, b))
    ##############################
   
    if returnResult: #when the stream is requesting the current result
        result = (0.0, 0.0, 0.0)
        #[TODO]#
        #any additional processing to return the result at this point
        sum_r = sum([x[0] for x in memory if x is not None])
        sum_g = sum([x[1] for x in memory if x is not None])
        sum_b = sum([x[2] for x in memory if x is not None])
        count = len([x for x in memory if x is not None])
        if count > 0:
            result = (sum_r / count, sum_g / count, sum_b / count)
        return result
    else: #no need to return a result
        pass


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
    n =0
    with open(filename, 'r') as infile:
        i = 0#keeps track of lines read
        for line in infile:
        
            #remove \n and convert to int
            element = line.strip()
            all.append(element)#DEBUG
            i += 1
            
            #call tasks         
            if i in printLines: #print status at this point: 
                result1a = task1A_meanRGBsStream(element,returnResult=True)
                print(" Result at stream element # %d:" % i)
                print("   1A:   means: %s" % str(["%.2f" % float(m) for m in result1a]))
                print(" [current memory size: %d]\n" % \
                    (getMemorySize(memory)))
                
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
    print("\n*******************************\n   Stream bloom Terminated \n*******************************")
    if peakMem > 0:
        print("(peak memory usage was: ", peakMem, ")")
