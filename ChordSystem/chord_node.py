


from multiprocessing.sharedctypes import Value


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
    def __init__(self, n, k, node=None):
        if not (0 <= n < NODES and 0 < k <= M):
            raise ValueError('invalid finger entry values')
        self.start = (n + 2 ** (k-1)) % NODES
        self.next_start = (n + 2 ** k) % NODES if k < M else n
        self.interval = ModRange(self.start, self.next_start, NODES)
        self.node = node
    
    def __repr__(self):
        return '<finger [{}, {}): {}>'.format(self.start, self.next_start, self.node)

    def __contains__(self, id_):
        return id_ in self.interval

class ChordNode(object):
    def __init__(self, port, buddy=None):
        self.node = Chord.lookup_addr(port)


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
        pass

    @staticmethod
    def lookup_node(n):
        if Chord.node_map is None:
            nm = {}

            for host in POSSIBLE_HOSTS:
                for port in POSSIBLE_PORTS:
                    addr = (host, port)
                    n = Chord.hash(addr, M)
                    if n in nm:
                        print('cannot use', addr, 'hash conflict', n)
                    nm[n] = addr
            Chord.node_map = nm
        return Chord.node_map[n]

    def lookup_addr(port, host='localhost'):
        addr = (host, port)
        try:
            Chord.lookup_node(0)
