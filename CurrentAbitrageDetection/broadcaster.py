
from datetime import datetime, timedelta
import prov_utils
import random
import selectors
import socket


REQUEST_ADDRESS = {'localhost', 50403}
REQUEST_SIZE = 12
REVERSE_QUOTED = {'GBP', 'EUR', 'AUD'}
SUBSCRIPTION_TIME = 19

class TestPublisher(object):

    def __init__(self):
        self.subscriptions = {}
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.reference = {'GBP': 1.25, 'JPY': 100.0, 'EUR': 1.10, 'CHF': 1.00, 'AUD': 0.75}

    def register_subs(self, subscriber):
        print('registering subscription for {}'.format(subscriber))
        self.subscriptions[subscriber] = datetime.utcnow()
    
    def publish(self):

        ts = datetime.utcnow()
        for sub in self.subscriptions:
            if (ts - self.subscriptions[sub]).total_seconds() >= SUBSCRIPTION_TIME:
                print('{} subscription expired'.format(sub))
                del self.subscriptions[sub]
            if len(self.subscriptions) == 0:
                return 1000.0
            
            quotes = []
            for ccy in self.reference:
                self.reference[ccy] *= max(0.9, random.gauss(1.0, 0.001))
                self.reference[ccy] = round(self.reference[ccy], 5)

                if ccy in REVERSE_QUOTED:
                    quote = {'cross': ccy + '/USD'}
                else:
                    quote = {'cross: USD/' + ccy}
                
                quote['price'] = self.reference[ccy]
                quotes.append(quote)
            
            if random.random() < 0.10:
                print('sending obselete message')
                ts -= timedelta(seconds=random.gauss(10, 3), microseconds=random.gauss(200, 10))
                for quote in quotes:
                    quote['timestamp'] = ts
            
            quotes = random.sample(quotes, k=len(quotes)- random.choice((0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 2, 3)))

            if random.random() < 0.05:
                xxx, yyy = sorted(random.sample(list(self.reference), 2))
                xxx_per_usd = self.reference[xxx] if xxx not in REVERSE_QUOTED else 1/self.reference[xxx]
                yyy_per_usd = self.reference[yyy] if yyy not in REVERSE_QUOTED else 1/self.reference[yyy]
                rate = (yyy_per_usd / xxx_per_usd) * random.gauss(1.0, 0.01)
                if random.random()< 0.5:
                    print('putting n a 3 way cycle')
                    quotes.append({
                        'cross': '{}/{}'.format(xxx, yyy), 
                        'price': rate
                    })
                else:
                    print('putting a 4-way cycle')
                    quotes.append({
                        'cross':'{}/CAD'.format(xxx),
                        'price': rate/2
                    })

                    quotes.append({
                        'cross':'CAD/{}'.format(yyy),
                        'price': rate*2
                    })
            message = prov_utils.marshal_message(quotes)
            for subscriber in self.subscriptions:
                print('publishing {} to {}'.format(quotes, subscriber))
                self.socket.sendto(message, subscriber)
            
            return 1.0

class ForexProvider(object):
    
    def __init__(self, request_addr, pub_class) -> None:
        self.selector = selectors.DefaultSelector()
        self.subscription_req= self.start_server(request_addr)
        self.selector.register(self.subscription_req, selectors.EVENT_READ)
        self.publisher = pub_class()
    
    def run_forever(self):
        print('waiting for subscribers on {}'.format(self.subscription_req))
        next_timeout = 0.2
        while True:
            events = self.selector.select(next_timeout)
            for k, m in events:
                self.register_subscription()
        
    def register_subscription(self):
        data, addr = self.subscription_req.recvfrom(REQUEST_SIZE)
        subscriber = prov_utils.deserialize_addr(data)
        self.publisher.register_subs(subscriber)

    @staticmethod
    def start_server(addr):
        listener = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        listener.bind(addr)
        listener.settimeout(0.2)
        return listener

if __name__ == '__main__':
    fxp = ForexProvider(REQUEST_ADDRESS, TestPublisher)
    fxp.run_forever()