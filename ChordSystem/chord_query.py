import sys

from chord_node import Chord

if len(sys.argv) != 4:
    print("Usage: python chord_query.py PORT QB YEAR")
    print("Example:")
    port = 31488  # any running node
    # key = ('russellwilson/2532975', 2016)
    key = ('tomfarris/2513861', 1947)
    print("python chord_query.py {} {} {}".format(port, key[0], key[1]))
    print()
else:
    port = int(sys.argv[1])
    key = (sys.argv[2], int(sys.argv[3]))

node = Chord.lookup_addr(port)
print('looking up', key, 'from node', node)
print(Chord.get_value(node, key))
