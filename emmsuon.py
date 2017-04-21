#!/usr/bin/env python
import time
import MeshDS

class FibonacciHeap:
    
    # internal node class 
    class Node:
        def __init__(self, data, delayval, uid):
            self.data = -data
            self.bw = data
            self.parent = self.child = self.left = self.right = None
            self.degree = 0
            self.mark = False
            self.delay = delayval
            self.id = uid
            self.freebw = data
            
    # function to iterate through a doubly linked list
    def iterate(self, head):
        node = stop = head
        flag = False
        while True:
            if node == stop and flag is True:
                break
            elif node == stop:
                flag = True
            yield node
            node = node.right
    
    # pointer to the head and minimum node in the root list
    root_list, min_node = None, None
    operationcount = 0
    consolidate_ts = time.time()
    
    # maintain total node count in full fibonacci heap
    total_nodes = 0
    total_operations = 0

    def GetTopListNodeCount (self):
        nodelist = [x for x in self.iterate()]
        return len(nodelist)

    def findmax(self, clist):
        maxval = 0
        head = clist[0]
        for i in xrange(0, len(clist)):
            if clist[i].degree > maxval:
                maxval = clist[i].degree
                head = clist[i]
        return head

    #find the max delay accross this tree
    def find_delay_and_BWTime (self, size):
        d = 0
        b = 0.0
        retval = []
        if self.root_list is None:
            retval.append(0)
            retval.append(0)
            return retval
        print "Count = ", self.total_nodes
        children = [x for x in self.iterate(self.root_list)]

        max_node = self.findmax(children)
        #print "Delay 1: ", max_node.delay
        d += max_node.delay
        v = float(max_node.data)
        v = -v
        t = float(size/v)
        loop_count = max_node.degree
        b += t
        for j in range(0, loop_count):            
            childlist = [x for x in self.iterate(max_node.child)]
            if len(childlist) == 0:
                retval.append(d)
                retval.append(b)
                return retval
            max_node = self.findmax(childlist)
            #print "Degree: ",max_node.degree,", Delay ",j+2,": ",max_node.delay
            d += max_node.delay
            v = float(max_node.data)
            v = -v
            t = float(size/v)
            b += t
            if max_node.child is None:
                retval.append(d)
                retval.append(b)
                return retval
            del childlist[:]
        retval.append(d)
        retval.append(b)
        return retval
                    
    # return min node in O(1) time
    def find_min(self):
        return self.min_node
        
    # extract (delete) the min node from the heap in O(log n) time
    # amortized cost analysis can be found here (http://bit.ly/1ow1Clm)
    def extract_min(self):
        z = self.min_node
        if z is not None:
            if z.child is not None:
                # attach child nodes to root list
                children = [x for x in self.iterate(z.child)]
                for i in xrange(0, len(children)):
                    self.merge_with_root_list(children[i])
                    children[i].parent = None
            self.remove_from_root_list(z)
            # set new min node in heap
            if z == z.right:
                self.min_node = self.root_list = None
            else:
                self.min_node = z.right
                self.consolidate()
            self.total_nodes -= 1

        operationcount += 1
        return z

    def extract_node(self, node):
        
        z = node
        
        if z == self.min_node:
            return self.extract_min()

        #if z == self.root_list:
        #    self.remove_from_root_list(z)
        #    return z

        if z is not None:
            if z.child is not None:
                # attach child nodes to root list
                children = [x for x in self.iterate(z.child)]
                for i in xrange(0, len(children)):
                    self.merge_with_root_list(children[i])
                    children[i].parent = None            
            # set new min node in heap
            #if z == z.right:
            #    self.min_node = self.root_list = None
            #else:
            #    self.min_node = z.right
            #    self.consolidate()
            #If has no parent, remove from root list
            if z.parent is None:
                self.remove_from_root_list(z)
            else:
                y = z.parent
                self.cut(z, y)
                self.cascading_cut(y)
            #if z.left is not None:
            #    z.left.right = z.right
            #if z.right is not None:
            #    z.right.left = z.left
            #if z.parent is not None:
            #    if z.left is not None:
            #        z.parent.child = z.left
            #    else:
            #        z.parent.child = z.right
            #self.total_nodes -= 1
            #self.remove_from_root_list(z)
        self.operationcount += 1
        return z

    # insert new node into the unordered root list in O(1) time
    def insert(self, data, delayval, uid):
        n = self.Node(data, delayval, uid)
        n.left = n.right = n
        self.merge_with_root_list(n)
        if self.min_node is None or n.data < self.min_node.data:
            self.min_node = n
        self.total_nodes += 1

        self.operationcount += 1
        MeshDS.meshnodes[uid] = n
        MeshDS.meshnodecount += 1
        return n
        
    # modify the data of some node in the heap in O(1) time
    def decrease_key(self, x, k):
        if k > x.data:
            return None
        x.data = k
        y = x.parent
        if y is not None and x.data < y.data:
            self.cut(x, y)
            self.cascading_cut(y)
        if x.data < self.min_node.data:
            self.min_node = x
        operationcount += 1
            
    # merge two fibonacci heaps in O(1) time by concatenating the root lists
    # the root of the new root list becomes equal to the first list and the second
    # list is simply appended to the end (then the proper min node is determined)
    def merge(self, h2):
        H = FibonacciHeap()
        H.root_list, H.min_node = self.root_list, self.min_node
        # fix pointers when merging the two heaps
        last = h2.root_list.left
        h2.root_list.left = H.root_list.left
        H.root_list.left.right = h2.root_list
        H.root_list.left = last
        H.root_list.left.right = H.root_list
        # update min node if needed
        if h2.min_node.data < H.min_node.data:
            H.min_node = h2.min_node
        # update total nodes
        H.total_nodes = self.total_nodes + h2.total_nodes
        return H
        
    # if a child node becomes smaller than its parent node we
    # cut this child node off and bring it up to the root list
    def cut(self, x, y):
        self.remove_from_child_list(y, x)
        y.degree -= 1
        self.merge_with_root_list(x)
        x.parent = None
        x.mark = False
    
    # cascading cut of parent node to obtain good time bounds
    def cascading_cut(self, y):
        z = y.parent
        if z is not None:
            if y.mark is False:
                y.mark = True
            else:
                self.cut(y, z)
                self.cascading_cut(z)
    
    # combine root nodes of equal degree to consolidate the heap
    # by creating a list of unordered binomial trees
    def consolidate(self):
        A = [None] * self.total_nodes
        nodes = [w for w in self.iterate(self.root_list)]
        for w in xrange(0, len(nodes)):
            x = nodes[w]
            d = x.degree
            while A[d] != None:
                y = A[d] 
                if x.data > y.data:
                    temp = x
                    x, y = y, temp
                self.heap_link(y, x)
                A[d] = None
                d += 1
            A[d] = x
        # find new min node - no need to reconstruct new root list below
        # because root list was iteratively changing as we were moving 
        # nodes around in the above loop
        for i in xrange(0, len(A)):
            if A[i] is not None:
                if A[i].data < self.min_node.data:
                    self.min_node = A[i]
        consolidate_ts = time.time()
        
    # actual linking of one node to another in the root list
    # while also updating the child linked list
    def heap_link(self, y, x):
        self.remove_from_root_list(y)
        y.left = y.right = y
        self.merge_with_child_list(x, y)
        x.degree += 1
        y.parent = x
        y.mark = False
        
    # merge a node with the doubly linked root list   
    def merge_with_root_list(self, node):
        if self.root_list is None:
            self.root_list = node
        else:
            node.right = self.root_list.right
            node.left = self.root_list
            self.root_list.right.left = node
            self.root_list.right = node
            
    # merge a node with the doubly linked child list of a root node
    def merge_with_child_list(self, parent, node):
        if parent.child is None:
            parent.child = node
        else:
            node.right = parent.child.right
            node.left = parent.child
            parent.child.right.left = node
            parent.child.right = node

        if parent.freebw >= node.bw:
            parent.freebw -= node.bw
        else:
            parent.freebw = 0
            MeshDS.meshnodes.pop(parent.id)
            MeshDS.meshnodecount -= 1
            #Remove self from mesh
            
    # remove a node from the doubly linked root list
    def remove_from_root_list(self, node):
        if node == self.root_list:
            self.root_list = node.right
        node.left.right = node.right
        node.right.left = node.left
        
    # remove a node from the doubly linked child list
    def remove_from_child_list(self, parent, node):
        if parent.child == parent.child.right:
            parent.child = None
        elif parent.child == node:
            parent.child = node.right
            node.right.parent = parent
        node.left.right = node.right
        node.right.left = node.left

        parent.freebw += node.bw
        MeshDS.meshnodes[parent.id] = parent
        #Add self to mesh
        
