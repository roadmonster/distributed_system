"""
Group Coordinator Daemon
Course project for distributed system
"""
import pickle
import socketserver
import sys

BUF_SZ = 1024

class GCD(socketserver.BaseRequestHandler):
    """
    GCD repond with a list of group member to contact
    """
    JOIN_RESPONSE = [{'host': 'cs1.seattleu.edu', 'port': 21313},
    {'host': 'cs2.seattleu.edu', 'port': 33313},
    {'host': 'localhost', 'port': 23015}]

    def handle(self) -> None:
        raw = self.request.recv(BUF_SZ)
        print(self.client_address)
        try:
            message = pickle.loads(raw)
        except (pickle.PickleError, KeyError):
            response = bytes('Expected a pickled message, got '+
            str(raw)[:100] + '\n', 'utf-8')
        else:
            if message != 'JOIN':
                response = pickle.dumps('Unexpected message ' + str(message))
            else:
                response = pickle.dumps(self.JOIN_RESPONSE)
        self.request.sendall(response)
    

if __name__ == '__main__':
    if(len(sys.argv) != 2):
        print("Usage: python gcd.py GCDPORT")
        exit(1)
    port = int(sys.argv[1])
    with socketserver.TCPServer(('', port), GCD) as server:
        server.serve_forever();
