from copyreg import pickle
from random import random
import time
import json
import socket
import pickle

from KThread import *
from Raft.messages import *
from follower_functions import acceptor
class Server(object):
    def __init__(self, id_) -> None:
        self.id = id_
        self.config_file = 'config-%d'.format(self.id)

        self.role = 'follower'
        self.commitIndex = 0
        self.lastApplied = 0

        self.leaderId = 0

        addr = json.load(open('config.json'))
        port_list = addr['AddressBook']
        running = addr['running']
        self.initial_state = addr['initial_state']
        self.addressbook = {}
        for id_ in running:
            self.addressbook[id_] = port_list[id_ - 1]
        
        self.load()

        self.port = self.addressbook[self.id]
        # deep copy of the peers list
        self.request_votes = self.peers[:]

        self.numVotes = 0
        self.oldVotes = 0
        self.newVotes = 0

        self.lastLogIndex = 0
        self.lastLogTerm = 0

        self.listener = KThread(target = self.listen, args = (acceptor, ))
        self.listener.start()

        self.during_change = 0
        self.newPeers = []
        self.new = None
        self.old = None
    
    def listen(self, on_accept):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        server_socket.bind(("", self.port))
        print('start listening')
        while True:
            data, addr = server_socket.recvfrom(1024)
            my_thread = KThread(target = on_accept, args=(self, data, addr))
            my_thread.start()
    
    def follower(self):
        print('Running as a follower')
        self.role = 'follower'
        self.last_update = time.time()
        election_timeout = 5 * random() + 5
        
        self.start_election()
        while True:
            self.last_update = time.time()
            election_timeout = 5 * random() + 5
            while time.time() - self.last_update <= election_timeout:
                pass

            if self.election.is_alive(0):
                self.election.kill()
            self.start_election()
    def load(self):
        initial_running = [1,2,3]

        try:
            with open(self.config_file) as f:
                serverConfig = pickle.load(f)
        except Exception as e:
            if self.id not in initial_running:
                serverConfig = ServerConfig(100, 0, -1, [],[])
            else:
                initial_running.remove(self.id)
                serverConfig = ServerConfig(100, 0, -1, [], initial_running)
        self.poolsize = serverConfig.poolsize
        self.currentTerm = serverConfig.currentTerm
        self.votedFor = serverConfig.votedFor
        self.log = serverConfig.log
        self.peers = serverConfig.peers
        self.majority = (len(self.peers) + 1) / 2 + 1

    def start_election(self):
        self.role = 'candidate'
        self.election = KThread(target=self.thread_election, args = ())
        if len(self.peers) != 0:
            self.currentTerm += 1
            self.votedFor = self.id
            self.save()
            self.numVotes = 1
            if self.during_change == 1:
                self.newVotes = 0
                self.oldVotes = 0
                if self.id in self.new:
                    self.newVotes = 1
                if self.id in self.old:
                    self.oldVotes = 1
            elif self.during_change == 2:
                self.newVotes = 0
                if self.id in self.new:
                    self.newVotes = 1
            self.election.start()

    def thread_election(self):
        print('Time out, start a new election with term %d'.format(self.currentTerm))
        self.role = 'candiate'
        self.request_votes = self.peers[:]
        sender = self.id

        while True:
            
            for peer in self.peers:
                if peer in self.request_votes:
                    content = str(self.lastLogTerm + ' ' + str(self.lastLogIndex))
                    msg = RequestVoteMsg(sender, peer, self.currentTerm, content)
                    data = pickle.dumps(msg)
                    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    sock.sendto(data ("", self.addressbook[peer]))
            time.sleep(1)
    
    def leader(self):
        print('Running as a leader')
        self.role = 'leader'
        self.nextIndex = {}
        self.matchIndex = {}
        for peer in self.peers:
            self.nextIndex[peer] = len(self.log) + 1
            self.matchIndex = 0
        self.append_entries()
    
    def append_entries(self):
        """
        this is a method leader node calls to send updates of nextIndex, matchIndex and commitIndex
        and current commands to followers
        Or to use this as a heartbeat to inform the follower that leader is alive
        """
        # infinitely loop to keep the heartbeat every 0.5 seconds
        while True:
            
            # current node is under change
            if self.during_change != 0:
                # check all peer and set those not have a nextIndex initial value
                for peer in self.peers:
                    if peer not in self.nextIndex:
                        self.nextIndex[peer] = len(self.log) + 1
                        self.matchIndex[peer] = 0
            
            for peer in self.peers:
                # my leader log is more advanced than this peer's status
                # means this peer could be following the older leader
                # I need to update them
                if len(self.log) >= self.nextIndex[peer]:
                    # prevLogIndex is nextIndex's previous
                    prevLogIndex = self.nextIndex[peer] - 1
                    if prevLogIndex != 0:
                        # prevLogTerm is the term of prevLogIndex
                        prevLogTerm = self.log[prevLogIndex].term
                    else:
                        prevLogTerm = 0
                    # the entries I need to send to this peer should be 
                    # from the prevLogIndex to most current ones, but I shall send one at a time
                    
                    entries = [self.log[prevLogIndex]]
                else:
                    entries = []
                    prevLogIndex = len(self.log)
                    if prevLogIndex != 0:
                        prevLogTerm = self.log[prevLogIndex - 1].term
                    else:
                        prevLogTerm = 0
                # create appendEntriesMsg and send it to the peers' addr
                content = AppendEntriesMsg(self.id, peer, self.currentTerm, entries, self.commitIndex, prevLogIndex, prevLogTerm)
                data = pickle.dumps(content)
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.sendto(data, ("", self.addressbook[peer]))
            time.sleep(0.5)

    def step_down(self):
        if self.role == 'candiate':
            print('candidate step down when higher term')
            self.election.kill()
            self.last_update = time.time()
            self.role = 'follower'
        elif self.role == 'leader':
            self.follower_state = KThread(target = self.follower, args=())
            self.follower_state.start()

    def save(self):
        serverConfig = ServerConfig(self.poolsize, self.currentTerm, self.votedFor, self.log, self.peers)
    
    def run(self):
        time.sleep(1)
        self.follower_state = KThread(target = self.follower, args = ())
        self.follower_state.start()


    

