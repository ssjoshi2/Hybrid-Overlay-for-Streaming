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

addcount = 0
delcount = 0


def ADD (master):
    ups = randint(5, 1050)
    delaylist = []
    logging.info("Adding")
    for i in range(0, master.rootcount):
        delaylist.append(randint(15,2650))
    
    #print master.rootcount, len(delaylist)

    retval = master.AddNode(randint(15, 250), delaylist, ups)
    logging.info("Adding done")
    global addcount
    addcount += 1
    return retval

def DEL(master):
    logging.info("Deleting")
    master.DeleteNodeAtRandom()
    logging.info("Deleting done")
    global delcount    
    delcount += 1

def main():

    master = Master()
    logging.basicConfig(filename='work.log',level=logging.DEBUG)

    #Init by filling root
    for i in range(0, ROOTCOUNT):
        master.AddRoot(randint(18, 21), 1024)


    #Init
    for i in range (1000):
        #print "Adding node"
        retval = ADD(master)
        #if retval == 2:                       
        #    print "Added as root"
        #elif retval == 1:
        #    print "Added to some root"
        if retval == 0:
            print "Failed????"

    while True:
        #0 - Insrt
        #1 - Delete
        #2 - Nothing
        #Decide what to do
        choice = randint (0, 2)
        if choice == 0:
            ADD(master)
        elif choice == 1:
            DEL(master)
        else:
            time.sleep(1)


if __name__ == '__main__':
    main()

