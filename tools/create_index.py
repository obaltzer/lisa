from shapely.geometry import Point, Polygon
from shapely import wkt
from rtree import Rtree

import sys
import struct

output = sys.argv[1]

data_filename = output + '.data'
index_filename = output + '.data.idx'

data = open(data_filename, 'wb')
index = open(index_filename, 'wb')
    
tree = Rtree(output, overwrite = 1)

id = 0
offset = 0

for l in sys.stdin:
    x = wkt.loads(l)
    tree.add(id, x.bounds)
    s = len(x.wkb)
    data.write(x.wkb)
    index.write(struct.pack('L', offset))
    offset += s
    id += 1
data.close()
index.close()
