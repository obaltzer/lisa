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
                              Select, Mux, Group, Sort, Join, Filter, \
                              Aggregate

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

class AttributeRename(object):
    def __init__(self, input_schema, names):
        self._schema = Schema()
        for a in input_schema:
            if a.name() in names:
                self._schema.append(Attribute(
                    names[a.name()], 
                    a.type()
                ))
            else:
                self._achema.append(a)
        self._input_schema = input_schema

    def schema(self):
        return self._schema

    def accepts(self, other_schema):
        return self._input_schema == other_schema

    def __call__(self, r):
        return r

class UniversalSelect(object):
    def __init__(self, input_schema, mapping):
        '''
        mapping = {
            'name': {
                'type': type, 
                'args': ['input', 'input', ...], 
                'function': function
            },
            ...
        }
        '''
        self._input_schema = input_schema
        self._schema = Schema()
        self._mapping = mapping
        self._f = []
        for name in mapping:
            # Create output schema type
            self._schema.append(Attribute(
                name,
                mapping[name]['type'],
            ))
            # Verify input schema and mapping
            for n in mapping[name]['args']:
                if n not in self._input_schema:
                    raise Exception('Incompatible schema.')

            self._f.append((
                [input_schema.index(n) for n in mapping[name]['args']],
                mapping[name]['function'],
            ))

    def schema(self):
        return self._schema

    def accepts(self, other):
        return self._input_schema == other

    def __call__(self, r):
        return tuple(
            f[1](*[r[i] for i in f[0]]) for f in self._f
        )

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

class SumAgeAggregator(object):
    def __init__(self, input_schema):
        self._input_schema = input_schema
        self._af = []
        for a in self._input_schema:
            if a.name() == 'age':
                # Add the age
                self._af.append((
                    0,
                    lambda x, v: x + v,
                ))
            else:
                # Everything else keep as is
                self._af.append((
                    None,
                    lambda x, v: v,
                ))

    def accepts(self, other):
        return self._input_schema == other

    def init(self):
        '''
        Initializes and resets the aggregation value.
        '''
        self._c = list(af[0] for af in self._af)

    def record(self):
        '''
        Returns the record that represents the current aggregation value.
        '''
        return tuple(self._c)

    def __call__(self, r):
        '''
        Adds the specified record to the aggregate value.
        '''
        for i, c in enumerate(self._c):
            self._c[i] = self._af[i][1](c, r[i])

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

query_grouper = Group(
    query_streamer.output(), 
    {'age': lambda a, b: a is b}
)

qselect = Select(
    query_grouper.output(), 
    AttributeRename(
        query_grouper.output().schema(),
        { 'age': 'age_range' }
    )
)

aggregate = Aggregate(
    data_accessor.output(),
    SumAgeAggregator(data_accessor.output().schema())
)

aselect = Select(
    aggregate.output(),
    UniversalSelect(
        aggregate.output().schema(),
        {
            'name_age': {
                'type': str,
                'args': ['name', 'age'],
                'function': lambda name, age: '%s --> %d' % (name, age),
            }
        }
    )
)

joiner = Join(qselect.output(), aselect.output())


result_stack = ResultStack(
#    aggregate.output(),
    joiner.output(),
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
    ('Data Accessor', data_accessor),
    ('Query Grouper', query_grouper),
    ('Joiner', joiner),
    ('Aggregate', aggregate),
    ('Query Streamer', query_streamer),
    ('Result Stack', result_stack),
    ('Query Select', qselect),
    ('Aggregate Select', aselect),
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
