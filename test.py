import sys

sys.path.insert(0, 'lib')

from threading import Thread
import time

from lisa.schema import Schema, Attribute
from lisa.data_source import CSVFile
from lisa.access_methods import FindIdentities, FindRange
from lisa.types import Interval
from lisa.mini_engines import ArrayStreamer, DataAccessor, ResultStack, \
                              Select, Mux

from lisa.stream import Demux

import signal, os

def alarm_handler(signum, frame):
    system('kill -9 %d' , os.getpid())

signal.signal(signal.SIGALRM, alarm_handler)
signal.alarm(5)


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
#        (IntInterval(1, 3),),
#        (IntInterval(2, 5),),
#        (IntInterval(2, 5),),
#        (IntInterval(1, 3),),
#        (IntInterval(2, 5),),
])

# schema definition of the data stream
data_schema = Schema()
data_schema.append(Attribute('name', str))
data_schema.append(Attribute('age', int))

# definition of the data source
data_source = CSVFile('test3.csv', data_schema)

# create a data accessor
data_accessor = DataAccessor(
    query_streamer.output(), 
    data_source,
    FindRange
)

demux = Demux(data_accessor.output())
name_age_combiner = NameAgeCombiner(demux.schema())

selects = []
for s in range(0, 20):
    selects.append(Select(demux, name_age_combiner))

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

tasks += [
    ('Query Streamer', query_streamer),
    ('Data Accessor', data_accessor),
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
