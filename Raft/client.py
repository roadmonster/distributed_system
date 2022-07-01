import pickle
import socket
import time
import uuid
import json

from messages import *
from KThread import *

"""
This is client class exposing show_state() and config_change and buy_ticket() functions
"""
class client(object):
    # maintain a global count for client number
    cnt = 0
    def __init_(self):
        # set client id from 1
        client.cnt = client.cnt+1
        self.id = client.cnt
        self.num_of_reply = 0
    
    def buyTickets(self, port, buy_msg, uuid):
        """
        build socket and send to remote port(in real world there is going to an ip address)
        read the reply from the socket, ignore the situation when we have more than one reply
        from the leader, handle the connection error with grace
        """
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        msg = Request(buy_msg, uuid)
        s.sendto(pickle.dumps(msg), ("", port))
        while 1:
            try:
                reply, addr = s.recvfrom(1024)
                if reply != '':
                    self.num_of_reply += 1
                    print(reply)
                if self.num_of_reply == 2:
                    break
            except Exception as e:
                print('Connection refused')
        s.close()
    
    def show_state(self, port):
        """
        func to show the current state of ticket pool
        """
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        msg = Request('show')
        s.sendto(pickle.dumps(msg), ("", port))
        while 1:
            try:
                reply, addr = s.recvfrom(1024)
                if reply != '':
                    print('Pool size: %d'.format(reply))
                    break
            except Exception as e:
                print('Connection error', e.with_traceback)

    def config_change(self, port, new_config, uuid):
        """
        for each config change, 
        1. we need to first contact leader
        and leader shall inform follower and reply to us
        2. after 1st reply from leader, we inform leader safe to commit
        """
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        #send phase 1 config change request
        msg = ConfigChange(new_config, uuid, 1)
        s.sendot(pickle.dumps(msg), ("", port))
        while 1:
            try:
                reply, addr = s.recvfrom(1024)
                if reply != '':
                    print(reply)
                    break
            except Exception as e:
                print('Connection error', e.with_traceback)
        
        # send phase 2 config request
        msg = ConfigChange(new_config, uuid, 2)
        s.sendot(pickle.dumps(msg), ("", port))
        while 1:
            try:
                reply, addr = s.recvfrom(1024)
                if reply != '':
                    print(reply)
                    break
            except Exception as e:
                print('Connection error', e.with_traceback)
        s.close()

def main():
    """
    entry point of the function
    """
    try:
        with open('config.json', 'r') as f:
            config = json.load(f)
        ports = config['AddressBook']
        num_ports = len(ports)
    except Exception as e:
        raise e

    while True:
        customer = client()
        server_id = input('Which datacenter do you want to connect to? 1 - %d'.format(num_ports))
        request = input('How can we help you? --')

        # based on the user choice, send different type of request using thread
        # also enable the thread to terminate by instaling traces
        if request == 'show':
            requestThread = KThread(target = customer.shown_state, args = (ports[server_id-1], ))
            timeout = 5
        elif request.split()[0] == 'change':
            uuid_ = uuid.uuid1()
            msg_split = request.split()
            new_config_msg = msg_split[1:]
            new_config = [int(item) for item in new_config_msg]
            print(new_config)
            requestThread = KThread(target = customer.config_change, args = (ports[server_id-1], new_config, uuid_))
            timeout = 20
        else:
            uuid_ = uuid.uuid1()
            requestThread = KThread(target = customer.buyTickets, args= (ports[server_id-1], request, uuid_))
            timeout = 5
        start_time = time.time()
        requestThread.start()

        while time.time() - start_time < timeout:
            if not requestThread.is_alive():
                break
        
        if requestThread.is_alive():
            print('Time out, try again')
            requestThread.kill()

if __name__ == '__main__':
    main()

