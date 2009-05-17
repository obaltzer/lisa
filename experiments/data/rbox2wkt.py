import sys

from shapely.geometry import Polygon
from shapely import wkt

trim = Polygon(((0, 0), (0, 1), (1, 1), (1, 0)))

# reading from standard input
f = sys.stdin

# read header header
h = f.readline()

# read number of vertices and faces
np, nf = f.readline().strip().split(' ')[:2]

vertices = []
for i in xrange(int(np)):
    x, y = f.readline().strip().split(' ')[:2]
    vertices.append((float(x), float(y)))

for i in xrange(int(nf)):
    try:
        v = f.readline().strip().split(' ')[1:]
        r = tuple(vertices[int(x)] for x in v)
        p = Polygon(r).intersection(trim)
        print p.wkt
    except:
        pass
