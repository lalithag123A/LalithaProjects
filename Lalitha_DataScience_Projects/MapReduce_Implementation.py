##########################################################################
# Implements a basic version of MapReduce intended to run
# on multiple threads of a single system. This implementation
# is simply intended as an instructional tool for students
# to better understand what a MapReduce system is doing
# in the backend in order to better understand how to
# program effective mappers and reducers. 
#
# MyMRSimulator is meant to be inheritted by programs
# using it. See the example "WordCountMR" class for 
# an exaample of how a map reduce programmer would
# use the MyMRSimulator system by simply defining
# a map and a reduce method. 
#
# Student Name: Lalitha Gorantla, XXXXX, XXXXX
# Student ID: 1323682, XXXXXX, XXXXX
##########################################################################

import sys
from abc import ABCMeta, abstractmethod
from multiprocessing import Process, Manager
from pprint import pprint
import numpy as np
import pandas as pd
from random import random
import math
import csv
##########################################################################
# MapReduceSystem: 

class MyMRSimulator:
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
        #print('data_chunk')
        #print(data_chunk)
        for (k, v) in data_chunk:
            #print('v')
            #print(k)
            #run mappers:
            chunk_kvs = self.map(k, v) #the resulting keys and values after running the map task
            mapped_kvs.extend(chunk_kvs) 
			
	#assign each kv pair to a reducer task
        if combiner:
            for_early_reduce = dict()#holds k, vs for running reduce
            #1. setup value lists for reducers
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
    def reduceTask(self, kvs, namenode_fromR, datasetInfo): 
        #SEGMENT 1. Sort such that all values for a given key are in a 
        #           list for that key 
        dict_keys = dict() 
        for k, vs in kvs:
        	try:
        		val = vs
        		if(isinstance(vs,int) or isinstance(vs,float)):        			
        			if k in dict_keys.keys():
        				if(datasetInfo == 'aquaculture-farmed-fish-production' or 
        					datasetInfo == 'OilSpills_byTankers'):
        					dict_keys[k] += val
        				else:
        					if (val > dict_keys[k]):
        						dict_keys[k] = val         					
        			else:
        				dict_keys[k] = val
        		else:   
        			dict_keys[k] = vs 
			        		
        	except KeyError:
        		val = vs
     		
        		
        #SEGMENT 2. call self.reduce(k, vs) for each key, providing 
        #           its list of values and append the results (if they exist) 
        #           to the list variable "namenode_fromR" 
        for (k, vs) in dict_keys.items():
        	namenode_fromR.append(self.reduce(k, vs))          
        pass

    def runSystem(self, datasetInfo): 
        #runs the full map-reduce system processes on mrObject

        #[SEGMENT 1]
        #the following two lists are shared by all processes
        #in order to simulate the communication
        namenode_m2r = Manager().list()   #stores the reducer task assignment and 
                                          #each key-value pair returned from mappers
                                          #in the form: [(reduce_task_num, (k, v)), ...]
                                          #[COMBINER: when enabled this might hold]
        namenode_fromR = Manager().list() #stores key-value pairs returned from reducers
                                          #in the form [(k, v), ...]
        
        #[SEGMENT 2]
        #divide up the data into chunks according to num_map_tasks, launch a new process
        #for each map task, passing the chunk of data to it. 
        #the following starts a process
        #      p = Process(target=self.mapTask, args=(chunk,namenode_m2r))
        #      p.start()  
        processes = []
        chunkSize = int(np.ceil(len(self.data) / int(self.num_map_tasks)))
        #splits the data into multiple process       
        i = 0
        data_split= []
        while True:
        	data_split.append(self.data[i: i+chunkSize])
        	i+=chunkSize
        	if i>=len(self.data):
        		break
        for chunk in data_split:
        	p = Process(target=self.mapTask, args=(chunk,namenode_m2r))
        	p.start()
        	processes.append(p)     
        #[SEGMENT 3]
        #join map task processes back
        for p in processes:
            p.join()
		#print output from map tasks 
        print("namenode_m2r after map tasks complete:")
        pprint(sorted(list(namenode_m2r)))

        ##[SEGMENT 4]
        #"send" each key-value pair to its assigned reducer by placing each 
        #into a list of lists, where to_reduce_task[task_num] = [list of kv pairs]
        to_reduce_task = [[] for i in range(self.num_reduce_tasks)]        
        i =0
        listkvpairs =[]
        for row in namenode_m2r:
        	to_reduce_task[row[0]].append(row[1])
        #[SEGMENT 5]
        #launch the reduce tasks as a new process for each. 
        processes = []
        for kvs in to_reduce_task:
             processes.append(Process(target=self.reduceTask, args=(kvs, namenode_fromR, datasetInfo)))
             processes[-1].start()
        #[SEGMENT 6]
        #join the reduce tasks back
        for p in processes:
            p.join()
        #print output from reducer tasks 
        print("namenode_fromR after reduce tasks complete:")
        tobesorted = list(namenode_fromR)
        sortedData = sorted(tobesorted)
        pprint(sortedData)
        if(datasetInfo == 'Maldives'):
        	with open('Maldives bleaching Index.csv', 'w') as Maldives_Max_output:
        		writer = csv.writer(Maldives_Max_output)
        		for key, value in sortedData:
        			writer.writerow([key, value])  
        			
        if(datasetInfo == 'Todos_os_Santos_Brazil'):
        	with open('Todos_os_Santos_Brazil bleaching Index.csv', 'w') as Todos_os_Santos_Brazil_Max_output:
        		writer = csv.writer(Todos_os_Santos_Brazil_Max_output)
        		for key, value in sortedData:
        			writer.writerow([key, value])  
        if(datasetInfo == 'aquaculture-farmed-fish-production'):
        	with open('aquaculture-farmed-fish-production-Index.csv', 'w') as aquaculture_farmed_fish_production_output:
        		writer = csv.writer(aquaculture_farmed_fish_production_output)
        		for key, value in sortedData:        			
        			writer.writerow([key, value])          			
        if(datasetInfo == 'OilSpills_byTankers'):
        	with open('OilSpills_byTankers-Index.csv', 'w') as OilSpills_byTankers_output:
        		writer = csv.writer(OilSpills_byTankers_output)
        		for key, value in sortedData:        			
        			writer.writerow([key, value])        	 
      	     			
        #return all key-value pairs:
        return namenode_fromR


##########################################################################
##Map Reducers:
 
            
class CountMR_FishFarming_CoastalReg(MyMRSimulator): 
    def map(self, k, v): 
        counts = dict()
        counts[k]=float(v)
        return counts.items()
    
    def reduce(self, k, vs): 
        return (k, round(np.sum(vs))) 
    
class MaxBleaching(MyMRSimulator):
    def map(self, k, v):  
        counts = dict()
        counts[k]=float(v)
        return counts.items()	
    def reduce (self, k, vs):        	 	
        return (k, vs)        

class OilSpills(MyMRSimulator):
    def map(self, k, v):  
        counts = dict()
        k = int(k)
        v = int(v.replace(',',''))
        if(k >= 1970 and k <= 1979):
        	k = 1970
        elif(k >= 1980 and k <= 1989):
        	k = 1980
        elif(k >= 1990 and k <= 1999):
        	k = 1990
        elif(k >= 2000 and k <=2009):
       		k = 2000
       	elif(k >= 2010 and k <= 2019):
       		k= 2010
       	elif(k >= 2020 and k <= 2022):
       		k = 2020        
        counts[k]=v
        return counts.items()	
    def reduce (self, k, vs):        	 	
        return (k, vs)  
##########################################################################

if __name__ == "__main__": #[Uncomment peices to test]

    print("\n\nTESTING MALDIVES CODE\n")
    
    ###################
    data = []
    count=0
    with open('Maldives.csv', mode='r', newline='') as csvReader:
    	reader=csv.reader(csvReader)
    	for eachr in reader:
    		if(count>0):    
	    		data.append((eachr[0],eachr[4]))
	    	count +=1  
	    	
	    	#if(count>50):#delete for full grab, debuggin
	    		#break;
	
	#print("\nExample of input data: ", data[:10]) #print only 10 rows
    
    #print(data)    
    #mrObjectNoCombiner = CountMR(data, 10, 3) #if we ever need sum of values, this can be used
    mrObjectNoCombiner = MaxBleaching(data, 10, 3)
    mrObjectNoCombiner.runSystem('Maldives')
    print("\n Count Basic WITH Combiner:")
    mrObjectWCombiner = MaxBleaching(data, 10, 3, use_combiner=True)
    mrObjectWCombiner.runSystem('Maldives')
   
    print("\n\nTESTING Todos_os_Santos_Brazil CODE\n")
    data = []
    count=0
    with open('Todos_os_Santos_Brazil.csv', mode='r', newline='') as csvReader:
    	reader=csv.reader(csvReader)
    	for eachr in reader:
    		if(count>0):    
	    		data.append((eachr[0],eachr[4]))
	    	count +=1  
	    	
	    	#if(count>50):#delete for full grab, debuggin
	    		#break;
	
	#print("\nExample of input data: ", data[:10]) #print only 10 rows
    
    #print(data)    
    #mrObjectNoCombiner = CountMR(data, 10, 3) #if we ever need sum of values, this can be used
    mrObjectNoCombiner = MaxBleaching(data, 10, 3)
    mrObjectNoCombiner.runSystem('Todos_os_Santos_Brazil')
    print("\n Count Basic WITH Combiner:")
    mrObjectWCombiner = MaxBleaching(data, 10, 3, use_combiner=True)
    mrObjectWCombiner.runSystem('Todos_os_Santos_Brazil')
    ###################
    #FISH FARMING IN COASTAL REGIONS   
    print("\n\nTESTING aquaculture-farmed-fish-production CODE\n")
    data = []
    count=0
    with open('aquaculture-farmed-fish-production.csv', mode='r', newline='') as csvReader:
    	reader=csv.reader(csvReader)
    	for eachr in reader:
    		if(count>0): 
    			data.append((eachr[0],eachr[3]))
    		count +=1 
    	#print(data)  
    	mrObjectNoCombiner = CountMR_FishFarming_CoastalReg(data, 10, 3)
    	mrObjectNoCombiner.runSystem('aquaculture-farmed-fish-production')
    	print("\n Count Basic WITH Combiner:")
    	mrObjectWCombiner = CountMR_FishFarming_CoastalReg(data, 10, 3, use_combiner=True)
    	mrObjectWCombiner.runSystem('aquaculture-farmed-fish-production')

    #####################
    #OIL SPILLS BY TANKERS
    print("\n\nTESTING OilSpills_byTankers CODE\n")
    data = []
    count=0
    with open('OilSpills_byTankers.csv', mode='r', newline='') as csvReader:
    	reader=csv.reader(csvReader)
    	for eachr in reader:
    		if(count>0): 
    			data.append((eachr[0],eachr[1]))
    		count +=1 
    	#print(data)  
    	mrObjectNoCombiner = OilSpills(data, 10, 3)
    	mrObjectNoCombiner.runSystem('OilSpills_byTankers')
    	print("\n Count Basic WITH Combiner:")
    	mrObjectWCombiner = OilSpills(data, 10, 3, use_combiner=True)
    	mrObjectWCombiner.runSystem('OilSpills_byTankers')    
    

  
