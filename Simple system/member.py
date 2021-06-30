"""
Member server
"""
from GCD import BUF_SZ
import sys
import socketserver
import pickle

BUF_SZ = 1024

class Member(socketserver.BaseRequestHandler):
    
    def handle(self):
        raw = self.request.recv(BUF_SZ)

        try:
            message = pickle.loads(raw)
        except(KeyError, pickle.PickleError):
            response = bytes('Expected picked message but got ' + 
            str(raw)[:100], 'utf-8')
        if message != 'Hello':
            response = pickle.dumps('Unsupported message ' + message)
        else:
            response = pickle.dumps('Ok, Hi ' + self.client_address)
        self.request.sendall(response)
    
if __name__ == '__main__':
    if(len(sys.argv) != 2):
        print('Usage: python Member.py PORT')
        exit(1)

    port = int(sys.argv[1])

    with socketserver.TCPServer(('', port), Member) as server:
        print('server running')
        server.serve_forever()


