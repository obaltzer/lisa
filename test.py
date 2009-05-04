import sys

sys.path.insert(0, 'lib')

from threading import Thread
import time

from lisa.schema import Schema, Attribute
from lisa.data_source import CSVFile, DBTable, Rtree
from lisa.access_methods import FindIdentities, FindRange
from lisa.types import Interval, Geometry
from lisa.mini_engines import ArrayStreamer, DataAccessor, ResultStack, \
                              Select, Mux

from lisa.stream import Demux

import signal, os

import sqlite3
from shapely import wkt

# some type definitions
IntInterval = Interval(int)

class NameAgeCombiner(object):
    def __init__(self, input_schema):
        self._schema = Schema()
        self._schema.append(Attribute('name_age', str))
        self._input_schema = input_schema
        self._indices = {
            'name': input_schema.index(Attribute('name', str)),
            'age': input_schema.index(Attribute('age', int))
        }

    def schema(self):
        return self._schema

    def accepts(self, other_schema):
        return self._input_schema == other_schema

    def __call__(self, r):
        return ('%s: %d' % (
            r[self._indices['name']],
            r[self._indices['age']]
        ), )

class NameAgeCombinerReverse(object):
    def __init__(self, input_schema):
        self._schema = Schema()
        self._schema.append(Attribute('name_age', str))
        self._input_schema = input_schema
        self._indices = {
            'name': input_schema.index(Attribute('name', str)),
            'age': input_schema.index(Attribute('age', int))
        }

    def schema(self):
        return self._schema

    def accepts(self, other_schema):
        return self._input_schema == other_schema

    def __call__(self, r):
        return ('%d: %s' % (
            r[self._indices['age']],
            r[self._indices['name']]
        ), )

# schema definition of the query stream
query_schema = Schema()
query_schema.append(Attribute('age', IntInterval))

# query stream generator from array
query_streamer = ArrayStreamer(query_schema, [
        (IntInterval(1, 3),),
        (IntInterval(2, 5),),
        (IntInterval(1, 3),),
        (IntInterval(1, 3),),
        (IntInterval(2, 5),),
        (IntInterval(2, 5),),
        (IntInterval(1, 3),),
        (IntInterval(2, 5),),
])

demux = Demux(query_streamer.output())

# schema definition of the data stream
data_schema = Schema()
data_schema.append(Attribute('name', str))
data_schema.append(Attribute('age', int))

data_schema.append(Attribute('rowid', int, True))
data_source = DBTable('test.db', 'person', data_schema)
print [x for x in data_source.intersect({'rowid': (20, 30)})]

rtree_source = Rtree('data/zip5', 'zip')
find_identities = FindIdentities(rtree_source)
find_range = FindRange(rtree_source)
querys = Schema()
querys.append(Attribute('oid', int))
print find_identities.query(querys, (10000,))

querys = Schema()
querys.append(Attribute('oid', IntInterval))
print [x for x in find_range.query(querys, (IntInterval(1, 3),))]

querys = Schema()
querys.append(Attribute('zip', Geometry))
print len([x for x in find_range.query(querys, (
    Geometry(wkt.loads(
        'POLYGON((-86.2 40, -86.0 40, -86.0 39.9, -86.2 39.9, -86.2 40))'
    ).wkb)
,))])

# print rtree_source[999999]

print len([x for x in rtree_source.intersect(
        {
#            'oid': (12345, 12360)
            'zip': Geometry(wkt.loads(
                'POLYGON((-86.2 40, -86.0 40, -86.0 39.9, -86.2 39.9, -86.2 40))'
            ).wkb)
        }
    )])

# definition of the data source
#data_source = CSVFile('test.csv', data_schema)

data_accessors = []
selects = []
for i in range(0, 1):
    # create a data accessor
    data_accessor = DataAccessor(
        demux, 
        data_source,
        FindRange
    )
    name_age_combiner = NameAgeCombiner(data_accessor.output().schema())
    selects.append(Select(data_accessor.output(), name_age_combiner))
    data_accessors.append(data_accessor)

mux = Mux(*[s.output() for s in selects])

#name_age_combiner_reverse = NameAgeCombinerReverse(demux.schema())
#select2 = Select(demux, name_age_combiner_reverse)

#name_age_combiner = NameAgeCombiner(data_accessor.output().schema())
#select = Select(data_accessor.output(), name_age_combiner)
#name_age_combiner_reverse = NameAgeCombinerReverse(data_accessor.output().schema())
#select2 = Select(data_accessor.output(), name_age_combiner_reverse)

result_stack = ResultStack(
#    query_streamer.output(),
    mux.output(),
#    data_accessor.output(),
)

def manage(task):
    print 'Running: ' + str(task)
    task.run()

tasks = []

tasks += [('Select', s) for s in selects]
tasks += [('Data Accessor', da) for da in data_accessors]

tasks += [
    ('Query Streamer', query_streamer),
    ('Result Stack', result_stack),
    ('Mux', mux),
]

threads = []

for t in tasks:
    threads.append(
        Thread(
            target = manage, 
            name = t[0], 
            args = (t[1],)
        )
    )

for t in threads:
    t.start()
