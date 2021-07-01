"""
Distributed System using selecor library
This is a free software released to the public
:Authors: Hao
:Version: 1.0
"""
import pickle
import selectors
import socket
import sys
from datetime import datetime
from enum import Enum

BACK_LOG = 100
BUF_SZ = 4096
TIMEOUT = 2.0
INTERVAL = 0.2
PEER_DIGITS = 100

class State(Enum):
    # vanishing socket
    DISABLED = 'DISABLED'

    # socket current outgoing message
    SEND_ELECTION = 'ELECTION'
    SEND_VICTORY = 'COORDINATOR'
    SEND_OK = 'OK'

    # Incoming message is pending
    WAITING_FOR_OK = 'WAIT_OK'
    WAITING_FOR_VICTOR = 'WHO IS THE WINNER'
    WAITING_FOR_ANY_MESSAGE = 'WAITING'

    def is_incoming(self):
        return self not in (State.SEND_ELECTION, State.SEND_OK, State.SEND_VICTORY)

class Ds2(object):

    def __init__(self, gcd_address, next_birth, su_id):
        """
        Ctor for Ds2 to talk to GCD

        :param gcd_address: host name and port
        :param next_birth: datetime of the next birthday
        :param su_id: seattle u id number
        """
        self.gcd_address = (gcd_address[0], int(gcd_address[1]))
        days_to_birthday = (next_birth - datetime.now()).days
        self.pid = (days_to_birthday, int(su_id))
        self.members = {} #dict pid:(ip, port)
        self.states = {} #dict socket:(state_name, time)
        self.bully = None # None means election is pending, otherwise be pid of leader
        self.selector = selectors.DefaultSelector()
        self.listener, self.listener_address = self.start_a_server()
    
    def start_a_server(self):
        """
        Start a socket bound to localhost at random port
        :return: listening socket and its address
        listener.getsockname() shall return socket's own address
        in format (host, port)
        """
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.bind(('localhost', 0))
        listener.listen(BACK_LOG)
        listener.setblocking(False)
        return listener, listener.getsockname()
    
    def join_group(self):
        """
        Join the group via the GCD.

        :raises TypeError: if we didn't get a reasonable response from GCD
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as gcd:
            message_data = (self.pid, self.listener_address)
            print('JOIN {}, {}'.format(self.gcd_address, message_data))
            gcd.connect(self.gcd_address)
            self.members = self.send(gcd, 'JOIN', message_data, wait_for_reply = True)
            if(type(self.members) != dict):
                raise TypeError('got unexpected data from gcd: {}'.format(self.members))
    
    @classmethod
    def send(cls, peer, message_name, message_data = None, wait_for_reply=False, buffer_size = BUF_SZ):
        """
        Pickles and sends the given message to the given socket
        and unpickles the returned value and returns it
        :param peer: socket to send/recv
        :param message name: message name such as 'ELECTION', 'COORDINATOR', etc.
        :param message data: contents of message such as (pid, (ip, port))
        :param wait_for_reply: true when waiting for synchronous reply
        :raises: pickle.dumps, socket.sendall, receive could raise
        """
        message = message_name if message_data is None else (message_name, message_data)
        peer.sendall(pickle.dumps(message))
        if wait_for_reply:
            return cls.receive(peer, buffer_size)
    
    @staticmethod
    def receive(peer, buffer_size = BUF_SZ):
        """
        recieve and unpickles message from the peer socket
        :param peer: peer socket
        :param buffer_size: buffer size of the socket.recv
        :return: the unpickled data
        :raises: whatever socket.recv or pickle.loads could raise
        """
        data = peer.recv(buffer_size)
        if not data:
            raise ValueError('socket closed')
        
        data = pickle.loads(data)
        if(type(data) == str):
            data = (data, None)
        return data

    def start_election(self, reason):

        print('Starting an election {}'.format(reason))
        self.set_leader(None)
        self.set_state(State.WAITING_FOR_OK)
        assume_myself_boss = True
        # iterates through keys (member pid) if greater than mine, contact 'Election'
        for member in self.members:
            if member > self.pid:
                peer = self.get_connection(member)
                if peer is None:
                    continue
                self.set_state(State.SEND_ELECTION, peer)
                assume_myself_boss = False
        if assume_myself_boss:
            self.declare_victory('no other bullies bigger than me')
    
    def get_connection(self, member):

        listener = self.members[member] # find the listening address of this member
        peer = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # set up socket
        peer.setblocking(True) # set non blocking
        try:
            # try connect to member address
            peer.connect(listener)
        except BlockingIOError:
            pass
        except Exception as err:
            print('FAILURE: get connection failed: {}'.format(err))
            return None

        return peer;

    def set_state(self, state, peer=None, switch_mod = False):

        print('{} : {}'.format(self.pr_sock(peer), state.name))
        if peer is None:
            peer = self
        
        if state.is_incoming():
            mask = selectors.EVENT_READ
        else:
            mask = selectors.EVENT_WRITE
        
        if state == State.DISABLED:
            if peer in self.states:
                if peer != self:
                    self.selector.unregister(peer)
                del self.states[peer]
            if len(self.states) == 0:
                print('{} (leader: {})\n'.format(self.pr_now(), self, self.pr_leader()))
                return
        
        if peer != self and peer not in self.states:
            peer.setblocking(False)
            self.selector.register(peer, mask)
        elif switch_mod:
            self.selector.modify(peer, mask)
        self.states[peer] = (state, datetime.now())

        # tyring sending right away
        if mask == selectors.EVENT_WRITE:
            self.send_message(peer)
    
    def send_message(self, peer):

        state = self.get_state(peer)
        print('{}: sending {} [{}]'.format(self.pr_sock(peer), state.value, self.pr_now()))

        try:
            # try sending state of current peer('ok', 'coordinator' and etc.)
            # as well as current knowledge of member to current connection
            # current connection's state set up by 'self.set_state()'
            self.send(peer, state.value, self.members)
        
        # if current connection error then close it
        except ConnectionError as err:
            print('Closing: {}'.format(err))
            self.disable(peer)
            return
        # otherwise regards it as temperorily blocked and return
        # to wait for next select
        # if connectd expired then close it
        except Exception as err:
            print('Failed {}: {}'.format(err.__class__.__name__, err))
            if self.is_expired(peer):
                print('timed out')
                self.disable(peer)
            return

        # if current connection(peer socket) waiting for ok, modify it in selctor
        # as waiting for ok(incoming) thus as selecotr.write
        # otherwise, close the connection
        if state == State.SEND_ELECTION:
            self.set_state(State.WAITING_FOR_OK, peer, switch_mod=True)
        else:
            self.disable(peer)
    
    def is_expired(self, peer=None, threshold=TIMEOUT):
        """
        check if current connection's state expired
        self.states[peer] = state_name, time
        check current time - time > threshold
        yes, then expire
        no valid
        """
        my_state, when = self.get_state(peer, detail= True)
        if my_state == State.DISABLED:
            return False
        waited = (datetime.now() - when).total_seconds()
        return waited > threshold
    
    def get_state(self, state, peer=None, detail=False):
        
        if peer is None:
            peer = self
        status = self.states[peer] if peer in self.states else (State.DISABLED, None)

        return status if detail else status[0]
    
    def disable(self, peer=None):
        self.set_state(State.DISABLED, peer)

    def declare_victory(self, reason):
        """
        :param reason: text msg for winning
        """
        print('Victory by {}. {}'.format(self.pid, reason))
        self.set_lader(self.pid)

        # iterate through members and send them except myself
        # message of winning(coordinate message) through set_state()
        for member in self.members:
            if member != self.pid:
                peer = self.get_connection(member)
                if peer is None:
                    continue
                self.set_state(State.SEND_VICTORY, peer)
        # after sending all connections winning, dump the connection
        self.disable()
    
    def set_leader(self, new_leader):
        """
        :param new_leader: listening tuple of the leader((pid):(ip, port))
        """
        self.bully = new_leader
        print('Leader is now {}'.format(self.pr_leader()))

    def update_members(self, their_idea_members):
        """
        if their idea is not none, then update mine one by one
        :param their_idea_members: dict {pid: listener} 
        """
        if their_idea_members is not None:
            for member in their_idea_members:
                self.members[member] = their_idea_members[member]
    
    def pr_sock(self, sock):
        """Printing helper for given socket."""
        if sock is None or sock == self or sock == self.listener:
            return 'self'
        return self.cpr_sock(sock)

    @staticmethod
    def cpr_sock(sock):
        """Static version of helper for printing given socket."""
        l_port = sock.getsockname()[1] % PEER_DIGITS
        try:
            r_port = sock.getpeername()[1] % PEER_DIGITS
        except OSError:
            r_port = '???'
        return '{}->{} ({})'.format(l_port, r_port, id(sock))

    def pr_leader(self):
        """Printing helper for current leader's name."""
        return 'unknown' if self.bully is None else ('self' if self.bully == self.pid else self.bully)

    def accept_peer(self):
        try:
            peer, _addr = self.listener.accept()

            print('{}: accepted [{}]'.format(self.pr_sock(peer), self.pr_now()))
            self.set_state(State.WAITING_FOR_ANY_MESSAGE, peer)
        except Exception as err:
            print('accept failed {}'.format(err))

    @staticmethod
    def pr_now():
        """Printing helper for current timestamp."""
        return datetime.now().strftime('%H:%M:%S.%f')

    def receive_message(self, peer):
        # TODO
        pass

    def check_timeouts(self):
        # TODO
        pass
    def run(self):
        self.join_group()
        self.selector.register(self.listener, selectors.EVENT_READ)
        self.start_election('at start up')

        while True:
            events = self.selector.select(INTERVAL)
            for key, mask in events:
                if key.fileobj == self.listener:
                    self.accept_peer()
                elif mask & selectors.EVENT_READ:
                    self.receive_message(key.fileobj)
                else:
                    self.send_message(key.fileobj)
            self.check_timeouts()
if __name__ == '__main__':
    if not 4 <= len(sys.argv) <= 5:
        print("Usage: python lab2.py GCDHOST GCDPORT SUID [DOB]")
        exit(1)
    if len(sys.argv) == 5:
        # assume ISO format for DOB, e.g., YYYY-MM-DD
        pieces = sys.argv[4].split('-')
        now = datetime.now()
        next_bd = datetime(now.year, int(pieces[1]), int(pieces[2]))
        if next_bd < now:
            next_bd = datetime(next_bd.year + 1, next_bd.month, next_bd.day)

        else:
            next_bd = datetime(2020, 1, 1)
        print('Next Birthday:', next_bd)
        su_id = int(sys.argv[3])
        print('SeattleU ID:', su_id)
        ds = Ds2(sys.argv[1:3], next_bd, su_id)
        ds.run()
    





