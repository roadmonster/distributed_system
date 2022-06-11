import ipaddress
import array
from datetime import datetime
from typing import Tuple

MAX_QUTOES_PER_MSG = 50
MICROS_PER_SECOND = 1_000_000

def serialize_price(x:float) ->bytes:
    a = array.array('d', [x])
    return a.tobytes() # render the floating number into 8-byte little endian byte array

def deserialize_addr(b:bytes) ->Tuple[str, int]:
    ip = ipaddress.ip_address(b[0:4])
    p = array.array('H')
    p.frombytes(b[4:6])
    p.byteswap() # convert back to host byte order should be little endian
    return str(ip), p[0]

def serialize_utcdatetime(utc:datetime) -> bytes:
    epoch = datetime(1970,1,1)
    micros = (utc - epoch).total_seconds() * MICROS_PER_SECOND
    a = array.array('Q', [int(micros)])
    a.byteswap() # convert to network byte order which is big endian
    return a.tobytes()

def marshal_message(quote_sequence) ->bytes:
    if len(quote_sequence) > MAX_QUTOES_PER_MSG:
        raise ValueError('max quotes exceeded for a single message')
    message = bytes()
    default_time = serialize_utcdatetime(datetime.utcnow())
    padding = b'\x00' * 10 # 10 bytes of zeros
    for quote in quote_sequence:
        if 'timestamp' in quote:
            message += serialize_utcdatetime(quote['timestamp'])
        else:
            message += default_time
        
        message += quote['cross'][0:3].encode('utf-8')
        message += quote['cross'][4:7].encode('utf-8')
        message += serialize_price(quote['price'])
        message += padding
    return message


