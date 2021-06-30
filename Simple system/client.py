"""
Client program
This is a free and unencumbered software
Author: Hao Li
Version: 1.0
"""

import sys
import pickle
import socket

class Client:
    """
    This program build socket and connect with gcd
    then unpickles resp from gcd into [{},{}]
    then contact each member within resp
    print out the unpickled resp from the member
    """
    def __init__(self, host, port) -> None:
        self.members = []
        self.host = host
        self.port = int(port)
        self.timeout = 1.5
    
    def join_group(self) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as gcd:
            address = (self.host, self.port)
            print('Join {}'.format(address))
            gcd.connect(address)
            self.members = self.message(gcd, 'JOIN')
            self.meet_members()
    
    @staticmethod
    def message(sock, data, buffer_size=1024):
        sock.sendall(data)
        return pickle.loads(sock.rev(buffer_size))

    def meet_members(self):
        for member in self.members:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as peer:
                peer.settimeout(self.timeout)
                address = (member['host'], member['port'])
                try:
                    peer.connect(address)
                except Exception as err:
                    print('Failed to connect {}'.format(err))
                
                response = self.message(peer, 'Hello')
                print(response)

if __name__ == '__main__':
    if(len(sys.argv) != 3):
        print('Usage: python client.py gcd_ip gcd_port')
        exit(1)
    gcd_ip = sys.argv[1]
    gcd_port = sys.argv[2]
    client = Client(gcd_ip, gcd_port)
    client.join_group()


