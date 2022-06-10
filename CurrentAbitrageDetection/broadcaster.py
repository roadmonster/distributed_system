
from datetime import datetime, timedelta
import random
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

            if random.random() < 0.95:
                xxx, yyy = sorted(random.sample(list(self.reference), 2))
                xxx_per_usd = self.reference[xxx] if xxx not in REVERSE_QUOTED else 1/self.reference[xxx]
                yyy_per_usd = self.reference[yyy] if yyy not in REVERSE_QUOTED else 1/self.reference[yyy]