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
                              Select, Mux, Group, Sort, Join, Filter
from lisa.info import ThreadInfo

from lisa.stream import Demux

import signal, os

import sqlite3
from shapely import wkt

# some type definitions
IntInterval = Interval(int)

#####################################################################
#
# Some helper classes. Primarily schema transformers and predicates.
#
#####################################################################

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

class FilterNameAge(object):
    def __init__(self, input_schema):
        self._input_schema = input_schema
        self._p = [
            (input_schema.index('name_age'), lambda x: x.find('F') >= 0),
        ]

    def accepts(self, other_schema):
        return self._input_schema == other_schema

    def __call__(self, r):
        for p in self._p:
#            print '--> %s : %s' % (r[p[0]], p[1](r[p[0]]))
            if not p[1](r[p[0]]):
                return False
        return True

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

# schema definition of the data stream
data_schema = Schema()
data_schema.append(Attribute('name', str))
data_schema.append(Attribute('age', int))

data_schema.append(Attribute('rowid', int, True))
data_source = DBTable('test.db', 'person', data_schema)

# create a data accessor
data_accessor = DataAccessor(
    query_streamer.output(), 
    data_source,
    FindRange
)
name_age_combiner = NameAgeCombiner(data_accessor.output().schema())
select = Select(data_accessor.output(), name_age_combiner)

query_grouper = Group(
    query_streamer.output(), 
    {'age': lambda a, b: a is b}
)

joiner = Join(query_grouper.output(), select.output())
filter = Filter(joiner.output(), FilterNameAge(joiner.output().schema()))

result_stack = ResultStack(
    filter.output(),
#    joiner.output(),
#    query_streamer.output(),
#    query_grouper.output(),
#    select.output(),
)

info_queue = Queue()

def manage(task):
    print 'Running: ' + str(task)
    task.run()
    info_queue.put(ThreadInfo())

tasks = []

tasks += [
    ('Select', select), 
    ('Data Accessor', data_accessor),
    ('Query Grouper', query_grouper),
    ('Joiner', joiner),
    ('Filter', filter),
    ('Query Streamer', query_streamer),
    ('Result Stack', result_stack),
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
