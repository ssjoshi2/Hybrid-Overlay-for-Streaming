#!/usr/bin/env python
from random import randint
from emmsuon import FibonacciHeap
import time
from collections import defaultdict
import threading
import thread
import logging

from Master import Master

ROOTCOUNT = 10
#in ms
DELAYTHRESHOLD = 20
#in MB/s
UPTHRESHOLD = 1000

class Root:
    def __init__(self, d, ups):
            self.delay = d
            self.upload = ups
            self.parent = self.child = None


def CollectStat (rootlist, rootlock, sleeptime):
    while True:
        maxd = 0
        t = 0
        time.sleep(sleeptime)
        logging.info('-------------------Starting-----------------')
        print '-------------------Starting-----------------'
        for i in range(len(rootlist)):
            rootlock[i].acquire()
            val = rootlist[i].child.find_delay_and_BWTime(5000)
            rootlock[i].release()
            dval = val[0]
            bval = val[1]
            if dval > maxd:
                maxd = dval
            if bval > t:
                t = bval
            info = "Max delay for root node ", i+1," = ",dval,"ms"
            logging.info(info)
    
        mxdinms = t*1000 + maxd
        info = "\nWorst delay for the whole network = ", maxd,"ms"
        logging.info(info)
        info = "\nTotal time to send 5000MB file on worst path = ", mxdinms/1000,"s"
        logging.info(info)


def main():    
    insert_time = float(0)
    count = 0
    id_to_node = defaultdict(None)
    
    GOD = Root (2, 10000)
    rootlist = []
    rootlock = []

    logging.basicConfig(filename='stats.log',level=logging.DEBUG)

    #Init by filling root
    for i in range(0,ROOTCOUNT):
        rootlist.append(Root(randint(18, 21), randint(999, 1020)))
        rootlock.append(threading.RLock())

    GOD.parent = GOD
    GOD.child = rootlist

    for i in range(len(rootlist)):
        rootlist[i].parent = GOD
        rootlist[i].child = FibonacciHeap()

    thread.start_new_thread(CollectStat, (rootlist, rootlock, 10))

    for i in range (50000):
        mindelay = 300
        minups = 2000
        ups = randint(100, 1050)
        ups = -ups
        pos = 0
        for i in range(len(rootlist)):
            delay = randint(15,260)
            if delay < mindelay:
                mindelay = delay
                pos = i

        #add node to root i
        #print "Ups: ", ups, " Delay: ", mindelay, " Inserting into: ", pos
        rootlock[pos].acquire()
        start_time = time.time()
        n = rootlist[pos].child.insert(ups, mindelay, count)
        insert_time += time.time()-start_time
        rootlock[pos].release()
        start_time=0
        id_to_node[count] = n
        count += 1

    #rootlock[0].acquire()
    #n = rootlist[0].child.insert(-2051, 10, count)
    #rootlock[0].release()
    #id_to_node[count] = n
    #count += 1
   
    tot = 0 
    for i in range (len(rootlist)):
        rootlock[i].acquire()
        f = rootlist[i].child
        tot += f.total_nodes
        rootlock[i].release()

    print "Total root nodes = ",len(rootlist)
    print "Total node count = ",tot
    for i in range (0,len(rootlist)):
        rootlock[i].acquire()
        print "Total nodes in root node ",i+1," = ",rootlist[i].child.total_nodes
        rootlock[i].release()

    print "\nDeleting 500 nodes"
        
    del_time = 0 
    for i in range(0,500):
        c = randint(0,len(rootlist)-1)
        r = randint(1,count-1)
        n = id_to_node[r]
        rootlock[c].acquire()
        start_time = time.time()
        rootlist[c].child.extract_node(n)
        del_time += time.time()-start_time
        rootlock[c].release()
	
    #tot = 0
    #for i in range (len(rootlist)):
    #    f = rootlist[i].child
    #    tot += f.total_nodes
    #print "After delete: Total count ",tot

    print "\nTo insert ",tot," and delete 500 it took ",insert_time+del_time," seconds"

   # for i in range (len(rootlist)):
        #print "Root:",i
   #     f = rootlist[i].child
        #f.consolidate ()
        #print [x.data for x in f.iterate(f.root_list)]
   #     print "Max val = ", f.min_node.data
    #    print "Count: ", f.total_nodes     
        
    print "--------------------------------------------------CONSOLIDATE--------------------------------------------------------------------"
    cons_time = 0
    for i in range (len(rootlist)):
        #print "Root:",i
        f = rootlist[i].child
        rootlock[i].acquire()
        start_time = time.time()
        f.consolidate ()
        cons_time += time.time()-start_time
        rootlock[i].release()

    print "Time to Consolidate = ", cons_time," seconds"
    print "Total time = ", insert_time+del_time+cons_time," seconds"

    while True:
        time.sleep(100)
   # print [x.data for x in f.iterate(f.root_list)]
   # for i in range (len(rootlist)):
        #print "Root:",i
   #     f = rootlist[i].child
   #     print "Max val = ", f.min_node.data

        #print [x.data for x in f.iterate(f.root_list)]
        #print "Max val = ", f.min_node.data   
        #print "Count: ", f.total_nodes 
    #f = FibonacciHeap()

    #f.insert(-10)
    #f.insert(-2)
    #f.insert(-15)
    #f.insert(-6)

    #m = f.find_min()
    #print m.data # 2

    #q = f.extract_min()
    #print q.data # 2

    #q = f.extract_min()
    #print q.data # 6

    #f2 = FibonacciHeap()
    #f2.insert(-100)
    #f2.insert(-56)

    #f3 = f.merge(f2)
    #x = f3.root_list.right # pointer to random node
    #f3.decrease_key(x, -1)

    # print the root list using the iterate class method
    #print [x.data for x in f3.iterate(f3.root_list)] # [10, 1, 56]

    #q = f3.extract_min()
    #print q.data # 1

if __name__ == '__main__':
    main()

