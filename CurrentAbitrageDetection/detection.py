from datetime import datetime
import math
import socket
from bellmanford import BellmanFord
from subsciber_utils import unmarshal_message, serialize_address

REQUEST_ADDR = ('localhost', 50403)
FRESH_TIME = 1.5
BUF_SZ = 2048

class Detection(object):
    def __init__(self, subscription_addr):
        self.subscription_addr = subscription_addr
        self.graph = BellmanFord()
        self.markets = {}
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.listener.bind(('localhost', 0))

    def run(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as subscribe:
            subscribe.sendto(serialize_address(*self.listener.getsockname()), self.subscription_addr)
            for i in range(10):
                ts = datetime.utcnow()
                for market in list(self.markets):
                    if (ts - self.markets[market]).total_seconds() > FRESH_TIME:
                        print('removing stale quote for {}'.format(market))
                        ccy1, ccy2 = market
                        try:
                            self.graph.remove_edge(ccy1, ccy2)
                            self.graph.remove_edge(ccy2, ccy1)
                            del self.markets[market]
                        except KeyError:
                            pass
                    message, _addr = self.listener.recvfrom(BUF_SZ)
                    records = unmarshal_message(message)

                    for ts, ccy1, ccy2, rate in records:
                        print(ts, ccy1, ccy2, rate)
                        market = (ccy1, ccy2)
                        if market not in self.markets or self.markets[market] < ts:
                           self.graph.add_edge(ccy1, ccy2, -math.log10(rate))
                           self.graph.add_edge(ccy2, ccy1, math.log10(rate))
                           self.markets[market] = ts
                        else:
                            print('ignoring out-of-sequence message')
                    
                    distance, path, negative_cycle = self.graph.shortest_path('USD', tolerance=1e-12)
                    if negative_cycle:
                        self.report_arbitrage(distance, path, negative_cycle)

    def report_arbitrage(self, distance, predecessor, final_edge):
        print('Arbitrage')
        cycle = [final_edge[1], final_edge[0]]
        while cycle[-1] != cycle[0]:
            cycle.append(predecessor[cycle[-1]])
            quantity = 100
            ccy = cycle.pop()
            print('\tstart start with {} {}'.format(ccy, quantity))
            while len(cycle) > 0:
                next_ccy = cycle.pop()
                rate = 10 ** -self.graph.edges[ccy][next_ccy]
                quantity *= rate
                print('\texchange {} for {} at {} --> {}{}'.format(ccy, next_ccy, rate, next_ccy, quantity))
                ccy = next_ccy
            print()
if __name__ == '__main__':
    detector = Detection(REQUEST_ADDR)
    detector.run()