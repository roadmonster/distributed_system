import csv
import os
from chord_node import Chord
import time
import sys

def populate_from_qb_file(port, filename):
    node = Chord.lookup_addr(port)
    print('reading data from ', filename)
    count = 0

    with open(filename, newline='') as datafile:
        for row in csv.DictReader(datafile):
            value = {}
            for field_name in row:
                field = row[field_name]
                if field == '--':
                    field = None
                elif field.isdecimal():
                    field = float(field)
                    if field.is_integer():
                        field = int(field)
                value[field_name] = field
            key = {value['Player Id'], value['Year']}
            print(count, 'populating', key, value)
            Chord.put_value(node, key, value)
            count += 1
            time.sleep(0.5)
if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('Usage: python chord_populate.py PORT FILENAME')
        print('Example:')
        port = 31488
        filename = '/Users/vlhao/Docutments/Career_Stats_Passing.csv'
        print("python chord_populate.py {} {}".format(port, filename))
    else:
        port = int(sys.argv[1])
        filename = os.path.expanduser(sys.argv[2])
    
    populate_from_qb_file(port, filename)
