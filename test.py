import sys

sys.path.insert(0, 'lib')

from threading import Thread, current_thread
from Queue import Queue
import time

from lisa.schema import Schema, Attribute
from lisa.data_source import CSVFile, DBTable, Rtree
from lisa.access_methods import FindIdentities, FindRange
from lisa.types import Interval, Geometry
from lisa.mini_engines import ArrayStreamer, DataAccessor, ResultStack, \
                              Select, Mux, Group, Sort
from lisa.info import ThreadInfo

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

#############################################################
#
# TEST 1
#
#############################################################

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

info_queue = Queue()

def manage(task):
    print 'Running: ' + str(task)
    task.run()
    info_queue.put(ThreadInfo())

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

for t in threads:
    t.join()

while not info_queue.empty():
    i = info_queue.get()
    print i
    info_queue.task_done()

#############################################################
#
# TEST 1
#
#############################################################

class SubSchema(object):
    def __init__(self, input_schema, output_attributes):
        self._input_schema = input_schema
        self._output_schema = Schema()
        self._indices = []
        for name in output_attributes.keys():
            i = self._input_schema.index(name)
            self._indices.append(i)
            self._output_schema.append(Attribute(
                output_attributes[name],
                self._input_schema[i].type()
            ))

    def schema(self):
        return self._output_schema

    def accepts(self, other_schema):
        return self._input_schema == other_schema

    def __call__(self, r):
        return tuple(r[i] for i in self._indices)
            
# schema definition of the query stream
query_schema = Schema()
query_schema.append(Attribute('oid', IntInterval))

# query stream generator from array
query_streamer = ArrayStreamer(query_schema, [
        (IntInterval(13, 65),),
        (IntInterval(2, 5),),
        (IntInterval(13, 65),),
        (IntInterval(1, 3),),
        (IntInterval(1, 20),),
        (IntInterval(2, 5),),
        (IntInterval(20, 5000),),
        (IntInterval(1, 3),),
        (IntInterval(2, 5),),
        (IntInterval(2, 5),),
        (IntInterval(13, 65),),
        (IntInterval(1, 3),),
        (IntInterval(1, 20),),
        (IntInterval(2, 5),),
        (IntInterval(20, 50),),
        (IntInterval(1, 3),),
        (IntInterval(2, 5),),
])


county_source = Rtree('data/counties', 'county')
zip_source = Rtree('data/zip5', 'zip')

data_accessors = []
county_selects = []
zip_selects = []
groups = []
sorts = []

# create a data accessor
county_accessor = DataAccessor(
    query_streamer.output(), 
    county_source,
    FindRange
)
data_accessors.append(county_accessor)

demux = Demux(county_accessor.output())

sub_schema = SubSchema(demux.schema(), {'county': 'zip'})
 
for i in range(0, 2):
    select = Select(demux, sub_schema)
    county_selects.append(select)
    zip_accessor = DataAccessor(
        select.output(),
        zip_source,
        FindRange
    )
    sub_schema2 = SubSchema(zip_accessor.output().schema(), {'oid': 'zip'})
    zip_select = Select(zip_accessor.output(), sub_schema2)
    zip_selects.append(zip_select)
    sort = Sort(
        zip_select.output(), 
        {'zip': lambda a, b: cmp(a / 100, b / 100)},
        # {'zip': None },
        # True
    )
    sorts.append(sort)
    group = Group(
        sort.output(), 
        {'zip': lambda a, b: (a / 1000) == (b / 1000)}
        # {'zip': None }
    )
    groups.append(group)
    data_accessors.append(zip_accessor)

mux = Mux(*[s.output() for s in groups])

result_stack = ResultStack(
#    query_streamer.output(),
    mux.output(),
#    data_accessor.output(),
)

tasks = []

tasks += [
    ('Select %02d' % (i), s) 
        for i, s in enumerate(county_selects + zip_selects)
]
tasks += [
    ('Data Accessor %02d' % (i), da) 
        for i, da in enumerate(data_accessors)
]

tasks += [ ('Group', o) for o in groups ]
tasks += [ ('Sort', o) for o in sorts ]

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

print 'Number of threads: %d' % (len(threads))

for t in threads:
    t.start()

for t in threads:
    t.join()

while not info_queue.empty():
    i = info_queue.get()
    print i
    info_queue.task_done()

#find_identities = FindIdentities(rtree_source)
#find_range = FindRange(rtree_source)
#querys = Schema()
#querys.append(Attribute('oid', int))
#print find_identities.query(querys, (10000,))
#
#querys = Schema()
#querys.append(Attribute('oid', IntInterval))
#print [x for x in find_range.query(querys, (IntInterval(1, 3),))]
#
#querys = Schema()
#querys.append(Attribute('zip', Geometry))
#print len([x for x in find_range.query(querys, (
#    Geometry(wkt.loads(
#        'POLYGON((-86.2 40, -86.0 40, -86.0 39.9, -86.2 39.9, -86.2 40))'
#    ).wkb)
#,))])
#
## print rtree_source[999999]
#
#print len([x for x in rtree_source.intersect(
#        {
##            'oid': (12345, 12360)
#            'zip': Geometry(wkt.loads(
#                'POLYGON((-86.2 40, -86.0 40, -86.0 39.9, -86.2 39.9, -86.2 40))'
#            ).wkb)
#        }
#    )])
#

