from shapely import wkb
from rtree import Rtree

import sys
import os
import mmap
import struct

class Geometry(object):
    def __init__(self, wkb):
        self._wkb = wkb
        self._geom = None

    def geom(self):
        if not self._geom:
            self._geom = wkb.loads(self._wkb)
        return self._geom

    def __repr__(self):
        return self.geom().wkt

data_filename = sys.argv[1] + '.data'
index_filename = sys.argv[1] + '.data.idx'
tree = Rtree(sys.argv[1])

data_file = open(data_filename, 'r+')
index_file = open(index_filename, 'r+')

index_file.seek(0, os.SEEK_END)
index_length = index_file.tell()
index_file.seek(0)

data_file.seek(0, os.SEEK_END)
data_length = data_file.tell()
data_file.seek(0)

query = tuple([float(v) for v in sys.argv[2:6]])

print query

long_size = struct.calcsize('L')

data = mmap.mmap(data_file.fileno(), 0)
index = mmap.mmap(index_file.fileno(), 0)

results = []

for i in range(1, 2):
    for id in tree.intersection(query):
        a = id * long_size
        b = a + 2 * long_size
        if b > index_length:
            first, = struct.unpack('L', index[a:index_length])
            second = data_length
        else:
            first, second = struct.unpack('LL', index[a:b])
        results.append(Geometry(data[first:second]))
        # geom = wkb.loads(data[first:second])
        # print geom.wkt

print 'Done retrieval: %d' % (len(results))

for r in results:
    s = str(r)

data.close()
index.close()


#
#for id in tree.intersection(query):
#    a = id * long_size
#    b = a + 2 * long_size
#    index_file.seek(a)
#    if a + 2 * long_size > index_length:
#        first, = struct.unpack('L', index_file.read(long_size))
#        second = data_length
#    else:
#        first, second = struct.unpack('LL', index_file.read(2 * long_size))
#    data_file.seek(first)
#    geom = wkb.loads(data_file.read(second - first))
#    print geom.wkt
