import hashlib
from http import client
import pickle
import socket
import sys
import threading
import time

M = 25
NODES = 2 ** M
BUF_SZ = 8192
BACKLOG = 100
TEST_BASE = 50144
POSSIBLE_HOSTS = ('localhost',)
POSSIBLE_PORTS = range(2 ** 16)

class ModRange(object):
    def __init__(self, start, stop, divisor):
        self.divisor = divisor
        self.start = start % self.divisor
        self.stop = stop % self.divisor

        if self.start < self.stop:
            self.intervals = (range(self.start, self.stop),)
        elif self.stop == 0:
            self.intervals = (range(self.start, self.divisor), )
        else:
            self.intervals = (range(self.start, self.divisor), range(0, self.stop))
    
    def __repr__(self):
        return '<mrange [{},{}) % {}]>'.format(self.start, self.stop, self.divisor)
    
    def __contains__(self, id_):
        for interval in self.intervals:
            if id_ in interval:
                return True
        return False
    
    def __len__(self):
        total = 0
        for interval in self.intervals:
            total += len(interval)
        return total

    def __iter__(self):
        return ModRangeIter(self, 0, -1)

class ModRangeIter(object):
    
    def __init__(self, modRange, i, j):
        self.mr, self.i, self.j = modRange, i, j
    
    def __iter__(self):
        return ModRangeIter(self.mr, self.i, self.j)
    
    def __next__(self):
        if self.j == len(self.mr.intervals[self.i]) - 1:
            if self.i == len(self.mr.intervals) - 1:
                raise StopIteration()
            else:
                self.i += 1
                self.j = 0
        else:
            self.j += 1
        return self.mr.intervals[self.i][self.j]


class FingerEntry(object):
    """
    object for each entry in finger table
    start wil be my (current_node_number + 2^(k-1)) % total_node_number
    end will be (current_node_number + 2^(k)) % total_node_number
    interval is the gap between the start and the end, since this could be loop, hence we use the 
    ModRange object we created before, and we could use its method and use it as a iterator 
    """
    def __init__(self, bucket_num, k, node=None):
        if not (0 <= bucket_num < NODES and 0 < k <= M):
            raise ValueError('invalid finger entry values')
        self.start = (bucket_num + 2 ** (k-1)) % NODES
        self.next_start = (bucket_num + 2 ** k) % NODES if k < M else bucket_num
        self.interval = ModRange(self.start, self.next_start, NODES)
        self.node = node
    
    def __repr__(self):
        return '<finger [{}, {}): {}>'.format(self.start, self.next_start, self.node)

    def __contains__(self, id_):
        return id_ in self.interval

class ChordNode(object):
    """
    Chord object storing own node addr, buddy's addr who brought it into the chord,
    finger table object, predecessor, and key, and own state of whether joined.

    This object will execute method find successor and find predecessor and search in
    its finger table for the closest next finger to the given node number(basic idea of the chord)
    """
    def __init__(self, port, buddy=None):
        # find the bucket # of the given port and assigned it as my node #
        self.node = Chord.lookup_addr(port)
        # create k entries in my finger table object
        # in case of self.node get returned None from the lookup_addr() which shall triger the FingerEntry() ctor to raise error
        # creating a list of None firstly
        self.finger = [None] + [FingerEntry(self.node, k) for k in range(1, M+1)]
        
        self.predecessor = None
        # remember the keys that I have, store them in a dict
        self.keys = {}
        # no joined to the group after creation
        self.joined = False
        # remember the buddy so that I could connect to it to join the group
        self.buddy = Chord.lookup_addr(buddy)

    def __repr__(self):
        fingers = '.'.join([str(self.finger[i].node) for i in range(1, M+1)])
        return '<{}:{}[{}]>'.format(self.node, self.predecessor, fingers)
    
    @property
    def successor(self):
        return self.finger[1].node

    @successor.setter
    def successor(self, id_):
        self.finger[1].node = id_

    def find_successor(self, id_):
        """
        To find the target id's successor, we first id's predecessor
        then we send rpc to the remote predecessor to look for its successor
        doing this because id's successor should be 
        """
        # get the predecessor of the id_ 
        node_num = self.find_predecessor(id_)
        # rpc  to get the successor of this predecessor
        return self.call_rpc(node_num, 'successor')
    def find_predecessor(self, id_):
        """
        find the id_'s predecessor
        look up node# that satisfy node# < id_ <node#.successor
        which means this node# is the predesssor of id_
        while the condition is not satisfied, we keey send rpc to get closest preceeding finger
        """
        node_num = self.node
        # if the id falls within my range of nodes(me -- my successor), I am its predecessor
        # otherwise keep iterating and recrusively call rpc to find the closest finger towards this id
        while id_ not in ModRange(node_num + 1, self.call_rpc(node_num, 'successor', id_) + 1, NODES):
            node_num = self.call_rpc(node_num, 'closest_preceding_finger', id_)
        return node_num

    def closest_preceding_finger(self, id_):
        """
        to find the preceding finger to id_, 
        search in my fingers backwards, 
        check each if it's node lies within my next node to id
        means this finger's node is preceding id_
        """
        for i in range(M, 0, -1):
            if self.finger[i].node in ModRange(self.node + 1, id_, NODES):
                return self.finger[i].node
        # if none in my fingers satisfies the condition node# < id_
        # then I shall return my node number becauses I iterate reversely, hence I should be the closes
        return self.node

    def join(self, node_num=None):
        """
        Node join the chord with help of existing node
        if node is none means I am the starter
        """
        print('{}.join({})'.format(self.node, node_num if node_num is not None else ''))
        time.sleep(1)
        # node_num is none means I am the starter of this chord
        # therefore I initialize my finger table and put each entry as myself, set my predecessor as myself
        if node_num is None:
            for i in range(1, M+1):
                self.finger[i].node = self.node
            self.predecessor = self.node
        # otherwise, call the init_finger_table with the buddy and update others
        else:
            self.init_finger_table(node_num)
            self.update_others()

    def init_finger_table(self, np):
        """
        initialze finger table with the help other existing node
        """
        print('{}.init_finger_table({})'.format(self.node, np))
        # talk to my buddy to find my successor which is before my first finger table
        self.successor = self.call_rpc(np, 'find_successor', self.finger[1].start)
        self.predecessor = self.call_rpc(self.successor, 'predecessor') # fetches other node's predecessor
        self.call_rpc(self.successor, 'predecessor', self.node) # this rpc call sets predecessor in other node
        for i in range(1, M):
            # incrementally build the finger table, if my next finger's gap start is between my node and my current finger's node
            # then we say next finger's node is same as mine
            if self.finger[i+1].start in ModRange(self.node, self.finger[i].node, NODES):
                self.finger[i+1].node = self.finger[i].node
            else:
                # otherwise, talk to buddy to find next finger's start's successor and make it my next finger's node
                self.finger[i + 1].node = self.call_rpc(np, 'find_successor', self.finger[i+1].start)
        print('init_finger_table:', self)

    def update_others(self):
        """
        update all other node that should have this node in their finger tables
        iterate through the finger table and find predecessor of each finger
        because the predecessor of this finger must have this finger in its finger table
        therefore we need to visit them and update in their table, hence the rpc call for update finger table
        update their i-th finger as my node number
        """
        for i in range(1, M+1):
            p = self.find_predecessor((1+ self.node - 2 ** (i-1) + NODES) % NODES)
            self.call_rpc(p, 'update_finger_table', self.node, i)

    
    def update_finger_table(self, s, i):
        """
        if s is i-th finger of n, update this node's finger table with s
        """
        # if i-th finger's start is the same as its node or provided node is in the gap between the start and i-th finger's node
        # then we do nothing. 
        if(self.finger[i].start != self.finger[i].node
            and s in ModRange(self.finger[i].start, self.finger[i].node, NODES)):

            self.finger[i].node = s
            p = self.predecessor

            # recursively call update finger table to my predecessors since I have updated my finger table
            self.call_rpc(p, 'update_finger_table', s, i)
            return str(self)
        return 'did nothing {}'.format(self)

    def get_value(self, key):
        if key in self.keys:
            return self.keys[key]
        else:
            id = Chord.hash(key, M)
            # each node only k:v from self.predecessor to my node
            # hence if this id already in my range, but my keys does not have it
            # then there is no such instance
            if id in ModRange(self.predecessor + 1, self.node + 1, NODES):
                return None
            else:
                # in case this id not within my range, I should send rpc to other node
                # to get value, the node peer to contact is the successor of this id
                np = self.find_successor(id)
                return self.call_rpc(np, 'get_value', key)

    def put_value(self, key, value):
        id = Chord.hash(key, M)

        if id in ModRange(self.predecessor + 1, self.node + 1, NODES):
            self.keys[key] = value
            return
        else:
            np = self.find_successor(id)
            return self.call_rpc(np, 'put_value', key, value)


    def call_rpc(self, n, method, arg1=None, arg2=None):
        """call a method on another node"""
        if n == self.node:
            return self.dispatch_rpc(method, arg1, arg2)
        return Chord.call_rpc(n, method, arg1, arg2)
    
    def dispatch_rpc(self, method, arg1, arg2):
        pr = '{}.{}({}{})'.format(self.node, method, arg1 if arg1 is not None else '',
        ',' + str(arg2) if arg2 is not None else '')
        print(pr)

        if method == 'get_value':
            result = self.get_value(arg1)
        elif method == 'put_value':
            result = self.put_value(arg1, arg2)
        elif method == 'successor':
            result = self.successor
        elif method == 'find_successor':
            result = self.find_successor(arg1)
        elif method == 'predecessor':
            if arg1 is None:
                result = self.predecessor
            else:
                self.predecessor = arg1
                print('\t{}.p = {}'.format(self.node, self.predecessor))
                result = str(self)
        elif method == 'closest_preceding_finger':
            result = self.closest_preceding_finger(arg1)
        elif method == 'update_finger_table':
            result = self.update_finger_table(arg1, arg2)
        else:
            raise ValueError('unknown rpc method {}'.format(method))
        
        print('\t{} --> {}'.format(pr, result))
    
    def handle_rpc(self, client):
        rpc = client.recv(BUF_SZ)
        method, arg1, arg2 = pickle.loads(rpc)
        result = self.dispatch_rpc(method, arg1, arg2)
        client.sendall(pickle.dumps(result))
        print(self)
    
    def serve_forever(self):
        my_addr = Chord.lookup_node(self.node)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.settimeout(5)
            server.bind(my_addr)
            server.listen(BACKLOG)
            print('{}.serve_forever({})'.format(self.node, my_addr))
            if not self.joined:
                threading.Thread(target=self.join, args=(self.buddy, )).start()
                self.joined = True
            while True:
                try:
                    client, client_addr= server.accept()
                    threading.Thread(target=self.handle_rpc, args=(client,)).start()
                except socket.timeout:
                    print(self)
class Chord(object):

    node_map = None

    @staticmethod
    def put_value(node, key, value):
        return Chord.call_rpc(node, 'put_value', key, value)
    
    @staticmethod
    def get_value(node, key):
        return Chord.call_rpc(node, 'get_value', key)

    @staticmethod
    def call_rpc(n, method, arg1=None, arg2=None):
        """call the function on the given node"""
        addr = Chord.lookup_node(n)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            print('{},{}({}{})'.format(n, method, arg1 if arg1 is not None else '', 
                                        ',' + str(arg2) if arg2 is not None else ''))
            sock.connect(addr)
            sock.sendall(pickle.dumps((method, arg1, arg2)))
            result = pickle.loads(sock.recv(BUF_SZ))
            print('\tresult:', result)
            return result


    @staticmethod
    def lookup_node(n):

        #in case the node_map is not initialized, we precompute all the combination of ports and hosts
        if Chord.node_map is None:
            # create a empty dict for node_map
            nm = {}
            for host in POSSIBLE_HOSTS:
                for port in POSSIBLE_PORTS:
                    addr = (host, port)
                    # get the hash of the address and check if already exists if already exists, shoot a message
                    # otherwise, put the (hash:addr) into the dict
                    n = Chord.hash(addr, M)
                    if n in nm:
                        print('cannot use', addr, 'hash conflict', n)
                    else:
                        nm[n] = addr
            # assign the dict to the class propery
            Chord.node_map = nm
        # look up in the precomputed node_map if not existed otherwise simply return the matching result
        # works since lookup_addr() call this function with its keys, impossible to encounter situation with entries
        return Chord.node_map[n]

    @staticmethod
    def lookup_addr(port, host='localhost'):
        addr = (host, port)
        try:
            # run the lookup_node() with whatever parameter to make sure node_map in Chord is initialized
            # hence no need to have return value
            Chord.lookup_node(0)
        except KeyError:
            pass
        for n in Chord.node_map:
            # return the bucket # that matching the addr proved
            if Chord.node_map[n] == addr:
                return n
        return None

    @staticmethod
    def hash(key, bits=None):
        # serialize the (host, port)
        p = pickle.dumps(key)
        # pass the bytes into the secure hash function and get string represenatation
        hb = hashlib.sha1(p).digest()
        # render the hashed bytes back to integer, with byte order reading as big endian
        h = int.from_bytes(hb, byteorder='big')
        
        # mod the chords number to get the actual hash bucket
        if bits is not None:
            h %=2 ** bits
        return h
        
if __name__ == '__main__':
    usage = """
    Usage: python chord_node.py PORT [BUDDY]
    Idea is to start various processes like this:
    $ python3 chord_node.py 34023 & # initialize node
    $ python3 chord_node.py 31488 34023 & # add second node using existing node as buddy
    $ python3 chord_node.py 60011 34023 &
    """
    print('CHORD: m=', M)
    if len(sys.argv) not in (2, 3):
        print(usage)
        exit(1)
    port = int(sys.arv[1])
    buddy = int(sys.argv[2]) if len(sys.sys.argv) > 2 else None
    print('starting node', Chord.lookup_addr(port), 'joining via buddy at port ', Chord.lookup_addr(buddy))
    ChordNode(port, buddy).serve_forever()