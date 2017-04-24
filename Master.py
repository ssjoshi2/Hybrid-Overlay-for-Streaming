#!/usr/bin/env python
from random import randint
from emmsuon import FibonacciHeap
import time
import datetime
from collections import defaultdict
import threading
import thread
import logging
import math
import MeshDS

OPERATIONMAX = 10

#Half an hour in secs
CONSOLIDATETS = 1800

MAXROOTRATIO = 5

#In mbps
ROOTBWTHRESHOLD = 500

class Root:
    def __init__(self, d, ups, uid):
        self.delay = d
        self.upload = ups
        self.parent = None
        self.child = FibonacciHeap()
        self.uid = uid
        self.id_to_node = defaultdict(None)

def CollectStat (master, sleeptime):    

    while True:
        #print master
        time.sleep(sleeptime)
        print '11-------------------Starting-----------------'
        master.masterlock.acquire()
        print 'Locked-------------------Collecting-----------------',master.totalnodecount
        maxd = 0
        t = 0
        logging.info('-------------------Starting-----------------')
        logging.info(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        info = "Total Root Nodes = ", master.rootcount
        logging.info(info)
        info = "Total Nodes = ", master.totalnodecount
        logging.info(info)
        print '-------------------Starting-----------------'
        #print master.rootlist
        #print master.rootlist[0].child.root_list
        for i in range(master.rootcount):
            #print i, master.rootcount
            master.rootlocklist[i].acquire()
            val = master.rootlist[i].child.find_delay_and_BWTime(5000)
            master.rootlocklist[i].release()
            dval = val[0]
            bval = val[1]
            if dval > maxd:
                maxd = dval
            if bval > t:
                t = bval
            info = "Max delay for root node ", i+1," having ", master.rootlist[i].child.total_nodes," = ",dval,"ms"
            logging.info(info)
        mxdinms = t*1000 + maxd
        info = "\nWorst delay for the whole network = ", maxd,"ms"
        logging.info(info)
        info = "\nTotal time to send 5000MB file on worst path = ", mxdinms/1000,"s"
        master.masterlock.release()
        info = "\nTotal pull based nodes = ", MeshDS.meshnodecount
        logging.info(info)



def Consolidate (master, sleeptime):
    while True:
        time.sleep(sleeptime)
        print 'Start-------------------Consolidatinging-----------------'
        master.masterlock.acquire()
        print 'Locked-------------------Consolidatinging-----------------',master.totalnodecount
        for i in range(0, master.rootcount):
            #if master.rootlist[i].child.GetTopListNodeCount() >= math.log(master.rootlist[i].child.total_nodes+1, 10):
            if master.rootlist[i].child.operationcount  >= OPERATIONMAX:
                master.rootlocklist[i].acquire()
                master.rootlist[i].child.consolidate()
                master.rootlist[i].child.operationcount = 0
                master.rootlocklist[i].release()
            elif time.time() - master.rootlist[i].child.consolidate_ts >= CONSOLIDATETS:
                master.rootlocklist[i].acquire()
                master.rootlist[i].child.consolidate()
                master.rootlist[i].child.operationcount = 0
                master.rootlocklist[i].release()
        master.masterlock.release()
        print 'End-------------------Consolidatinging-----------------'


class Master:
    def __init__(self):
        logging.basicConfig(filename='stats.log',level=logging.DEBUG)
        print self
        thread.start_new_thread(Consolidate, (self, 10))
        thread.start_new_thread(CollectStat, (self, 15))

    id_to_root = defaultdict(None)
    rootlist = []
    rootlocklist = []
    rootcount = 0
    totalnodecount = 0
    #Only for root list
    masterlock = threading.Lock()

    def AddRoot(self, delay, bandwidth):
        self.masterlock.acquire()
        n = Root(delay, bandwidth, self.rootcount)
        self.id_to_root[self.rootcount] = n
        self.rootlocklist.append(threading.Lock())
        self.rootlist.append(n)
        #print self.rootlist
        self.rootcount += 1
        #print self.rootlist[self.rootcount - 1].child.root_list
        self.masterlock.release()

    def GetRootList (self):
        return self.rootidlist

    def GetRootNodeCount (self):
        return self.rootcount

    def DeleteRoot(self, rootid):
        self.masterlock.acquire()
        rootnode = self.id_to_root[rootid]
        self.rootcount -= 1
        #TODO:Reassign all children nodes in the fib heap
        self.masterlock.release()

    def GetnSmallest(self, dlist, n):
        return sorted(dlist)[n]

    def AddNode (self, masterdelay, delaylist, bandwidth):
        if self.rootcount < ((self.totalnodecount + self.rootcount) * MAXROOTRATIO)/100 and bandwidth >= ROOTBWTHRESHOLD:
            self.AddRoot (masterdelay, bandwidth)
            #print "OKOKOKOK"            
            return 2

        if len(delaylist) == self.rootcount:
            smallest = 0
            dididoit = False
            while smallest < self.rootcount:
                #Get the best rot node to add to
                val = self.GetnSmallest(delaylist, smallest)
                index = delaylist.index(val)

                self.masterlock.acquire()
                bestrootnode = self.rootlist[index]
                #Now check if tree node count has crossed threshold (log(N)), need to add one to total count to avoid log(0)
                self.rootlocklist[bestrootnode.uid].acquire()
                if bestrootnode.child.total_nodes > 0 and bestrootnode.child.total_nodes >= 2*math.log(int(self.totalnodecount+1), 10):
                    #print "HUHUHUH???", bestrootnode.child.total_nodes, math.log(int(self.totalnodecount+1), 10)
                    smallest += 1
                else:
                    #Found best root. add the node
                    n = bestrootnode.child.insert(delaylist[index], bandwidth, self.totalnodecount)
                    bestrootnode.id_to_node[self.totalnodecount] = n
                    self.totalnodecount += 1
                    dididoit = True
                self.rootlocklist[bestrootnode.uid].release()
                self.masterlock.release()

            if dididoit == False:
                #Add as a Root
                #TODO: WTF do we do there
                #print "WTF"
                self.AddRoot(masterdelay, bandwidth)
                return 2

            return 1
        else:

            print "HERR", len(delaylist), self.rootcount
            return 0

    def DeleteNodeAtRandom (self):

        self.masterlock.acquire()
        
        if self.rootcount <= 0:
            return

        while True:
            index = randint(0, self.rootcount-1)

            rootnode = self.rootlist[index]
            if rootnode.child.total_nodes > 0:
                #Get random node now
                print "Del",rootnode.child.total_nodes
                nid = randint(0, rootnode.child.total_nodes - 1)
                if rootnode.id_to_node.get(nid,None) is not None:
                    self.rootlocklist[rootnode.uid].acquire()
                    rootnode.child.extract_node(rootnode.id_to_node[nid])
                    rootnode.id_to_node.pop(nid, None)
                    self.rootlocklist[rootnode.uid].release()
                    self.totalnodecount -= 1
                    break
            else:
                break
        print("Releasing")
        self.masterlock.release()
