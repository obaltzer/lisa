import sys
import os.path

sys.path.insert(0, '../lib')

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

IntInterval = Interval(int)

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

input_file = sys.argv[1]

#############################################################
#
# TEST 1
#
#############################################################

# Schema definition of the query stream: an interval across all species
# IDs.
query_schema = Schema()
query_schema.append(Attribute('species.id', IntInterval))

# Schema definition of the species record stream.
species_schema = Schema()
species_schema.append(Attribute('species.id', int))

# Schema definition of the plant record stream.
plants_schema = Schema()
plants_schema.append(Attribute('plants.id', int))
plants_schema.append(Attribute('plants.height', int))
plants_schema.append(Attribute('plants.age', int))
plants_schema.append(Attribute('plants.species_id', int, True))

# Filter plants to only include those 10 years or older and 50 years or
# younger.
class FilterAge(object):
    def __init__(self, input_schema):
        self._input_schema = input_schema
        self._p = [
            (
                input_schema.index('plants.age'), 
                lambda x: x >= 10 and x <= 50
            ),
        ]

    def accepts(self, other_schema):
        return self._input_schema == other_schema

    def __call__(self, r):
        for p in self._p:
            if not p[1](r[p[0]]):
                return False
        return True

class MaxHeightAggregator(object):
    def __init__(self, input_schema):
        self._input_schema = input_schema
        self._af = []
        for a in self._input_schema:
            if a.name() == 'plants.height':
                # Only keep the maximum
                self._af.append((
                    0,
                    lambda x, v: x >= v and x or v,
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
        self._calls = 0

    def record(self):
        '''
        Returns the record that represents the current aggregation value.
        '''
        return tuple(self._c)

    def count(self):
        return self._calls

    def __call__(self, r):
        '''
        Adds the specified record to the aggregate value.
        '''
        self._calls += 1
        for i, c in enumerate(self._c):
            self._c[i] = self._af[i][1](c, r[i])

# The query stream contains only a single query.
query_streamer = ArrayStreamer(query_schema, [
        (IntInterval(0, int(1E10)),),
])

# Create a species data source: a  table in the input database.
species_source = DBTable(input_file, 'species', species_schema)
# Data accessor for the species data source.
species_accessor = DataAccessor(
    query_streamer.output(), 
    species_source,
    FindRange
)

demux = Demux(species_accessor.output())

engines = []
mux_streams = []
for i in range(0, 4):
    channel = demux.channel()
    
    # Select only the species ID for querying plants.
    species_id_select = Select(
        channel,
        UniversalSelect(
            species_accessor.output().schema(),
            {
                'plants.species_id': {
                    'type': int,
                    'args': ['species.id'],
                    'function': lambda v: v
                }
            }
        )
    )
    engines.append(species_id_select)
    # Data source for the plants.
    plants_source = DBTable(input_file, 'plants', plants_schema)
    # Data accessor for the plants data source.
    plants_accessor = DataAccessor(
        species_id_select.output(), 
        plants_source,
        FindIdentities
    )
    engines.append(plants_accessor)

    plants_filter = Filter(
        plants_accessor.output(),
        FilterAge(plants_accessor.output().schema())
#        plants_accessor.output(),
#        FilterAge(plants_accessor.output().schema())
    )
    engines.append(plants_filter)

    # Select only the species ID for querying plants.
    plants_height_select = Select(
        plants_filter.output(),
        UniversalSelect(
            plants_filter.output().schema(),
            {
                'plants.height': {
                    'type': int,
                    'args': ['plants.height'],
                    'function': lambda v: v
                }
            }
        )
    )
    engines.append(plants_height_select)

    plants_height_aggregate = Aggregate(
        plants_height_select.output(),
        MaxHeightAggregator(plants_height_select.output().schema())
    )
    engines.append(plants_height_aggregate)

    species_id_grouper = Group(
        channel, 
        {'species.id': lambda a, b: a == b}
    )
    engines.append(species_id_grouper)

    joiner = Join(species_id_grouper.output(), plants_height_aggregate.output())
    engines.append(joiner)
    mux_streams.append(joiner.output())

mux = Mux(*mux_streams)

result_stack = ResultStack(
    mux.output(),
#    plants_accessor.output()
#    plants_filter.output()
#    plants_height_select.output()
)

info_queue = Queue()

def manage(task):
    print 'Running: ' + str(task)
    task.run()
    info_queue.put(ThreadInfo())

tasks = []

tasks += [('engine', e) for e in engines]

tasks += [
    ('Query Streamer', query_streamer), 
    ('Species Accessor', species_accessor),
#    ('Species ID Select', species_id_select),
#    ('Plants Accessor', plants_accessor),
    ('Mux', mux),
#    ('Plants Filter', plants_filter),
#    ('Plants Height Select', plants_height_select),
#    ('Plants Height Max', plants_height_aggregate),
#    ('Species ID Group', species_id_grouper),
#    ('Joiner', joiner),
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
#    t.daemon = True
    t.start()

# time.sleep(10)

for t in threads:
    t.join()

while not info_queue.empty():
    i = info_queue.get()
    print i
    info_queue.task_done()
