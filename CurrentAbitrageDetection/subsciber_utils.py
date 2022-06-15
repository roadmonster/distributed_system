from array import array
from datetime import datetime
import ipaddress
from multiprocessing.sharedctypes import Value
RECORD_LEN = 32

def deserialize_price(b:bytes) -> float:
    """
    convert an 8-byte section of currency exchange message as a floating point number
    """
    a = array('d')
    a.frombytes(b) # frombytes shall render from bytes back to float number
    return a[0]

def deserialize_utcdatetime(b:bytes) ->datetime:
    a = array('Q')
    a.frombytes(b)
    a.byteswap()
    micros = a[0]
    seconds = micros // 1_000_000
    fractional = micros % 1_000_000
    approx = datetime.utcfromtimestamp(seconds)
    return datetime(approx.year, approx.month, approx.day, approx.hour, approx.minute, approx.second, fractional)


def serialize_address(host_ip: str, port: int) -> bytes:
    ip = ipaddress.ip_address(host_ip)
    if ip.version != 4:
        raise ValueError('Forex Provider only supports IPv4')
    p = array('H', [port])
    p.byteswap()
    return ip.packed + p.tobytes()

def unmarshal_message(message:bytes):
    records = []
    for i in range(0, len(message), RECORD_LEN):
        record = message[i:i + RECORD_LEN]
        ts = deserialize_utcdatetime(record[:8])
        ccy1 = record[8:11].decode('utf-8')
        ccy2 = record[11:14].decode('utf-8')
        price = deserialize_price(record[14:22])
        records.append((ts, ccy1, ccy2, price))
    return records