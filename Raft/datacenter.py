from copyreg import pickle
from random import random
import time
import json
import socket
import pickle

from KThread import *
from Raft.messages import ServerConfig
class Server(object):
    def __init__(self, id_) -> None:
        self.id = id_
        self.config_file = 'config-%d'.format(self.id)

        self.role = 'follower'
        self.commitIndex = 0
        self.lastApplied = 0

        self.leaderId = 0

        addr = json.load(file('config.json'))
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

        

    

