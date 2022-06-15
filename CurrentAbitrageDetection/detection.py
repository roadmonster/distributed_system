import socket
from bellmanford import BellmanFord
REQUEST_ADDR = ('localhost', 50403)
class Detection(object):
    def __init__(self, subscription_addr):
        self.subscription_addr = subscription_addr
        self.graph = BellmanFord()
        self.markets = {}
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.listener.bind(('localhost', 0))

    def run(self):
        pass

    def report_arbitrage(self, distance, predecessor, final_edge):
        pass

if __name__ == '__main__':
    detector = Detection(REQUEST_ADDR)
    detector.run()