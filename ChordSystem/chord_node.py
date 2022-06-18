import hashlib
import pickle
import socket
import sys
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
    def __init__(self, bucket_num, k, node=None):
        if not (0 <= bucket_num < NODES and 0 < k <= M):
            raise ValueError('invalid finger entry values')
        self.start = (bucket_num + 2 ** (k-1)) % NODES
        self.next_start = (bucket_num + 2 ** k) % NODES if k < M else n
        self.interval = ModRange(self.start, self.next_start, NODES)
        self.node = node
    
    def __repr__(self):
        return '<finger [{}, {}): {}>'.format(self.start, self.next_start, self.node)

    def __contains__(self, id_):
        return id_ in self.interval

class ChordNode(object):
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
        while id_ not in ModRange(node_num + 1, self.call_rpc(node_num, 'successor', id_), NODES):
            node_num = self.call_rpc(node_num, 'closest_preceding_finger', id_)
        return node_num

    def closest_preceding_finger(self, id_):
        """
        iteratre from the furthest finger to the closest
        """
        for i in range(M, 0, -1):
            if self.finger[i].node in ModRange(self.node + 1, id_, NODES):
                return self.finger[i].node
        # if none in my fingers satisfies the condition node# < id_
        # then I shall return my node number becauses I iterate reversely, hence I should be the closes
        return self.node

    def join(self, node_num=None):
        print('{}.join({})'.format(self.node, node_num if node_num is not None else ''))
        time.sleep(1)
        if node_num is None:
            for i in range(1, M+1):
                self.finger[i].node = self.node
            self.predecessor = self.node
        else:
            self.init_finger_table(node_num)
            self.update_others()

    def init_finger_table(self, np):
        print('{}.init_finger_table({})'.format(self.node, np))
        self.successor = self.call_rpc(np, 'find_successor', self.finger[1].start)
        self.predecessor = self.call_rpc(self.successor, 'predecessor', self.node)
        self.call_rpc(self.successor, 'predecessor', self.node)
        for i in range(1, M):
            if self.finger[i+1].start in ModRange(self.node, self.finger[i].node, NODES):
                self.finger[i+1].node = self.finger[i].node
            else:
                self.finger[i + 1].node = self.call_rpc(np, 'find_successor', self.finger[i+1].start)
        print('init_finger_table:', self)

    
    def call_rpc(self, n, method, arg1=None, arg2=None):
        """call a method on another node"""
        if n == self.node:
            return self.dispatch_rpc(method, arg1, arg2)
        return Chord.call_rpc(n, method, arg1, arg2)
    
    def dispatch_rpc(self, method, arg1, arg2):
        pass

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