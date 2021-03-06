import sys

# Setup the package search path.
sys.path.insert(0, '../lib')

from threading import Thread, current_thread
from Queue import Queue

from lisa.schema import Schema, Attribute
from lisa.data_source import DBTable
from lisa.access_methods import FindIdentities, FindRange
from lisa.types import IntInterval
from lisa.mini_engines import ArrayStreamer, DataAccessor, ResultStack, \
                              Select, Mux, Group, Join, Filter, \
                              Aggregate, ResultFile
from lisa.stream import Demux
from lisa.util import UniversalSelect
from lisa.info import ThreadInfo

tracks = int(sys.argv[1])
input_file = sys.argv[2]

#############################################################
#
# Query 1
#
# SELECT species.id, MAX(plants.height) 
# FROM species 
# LEFT JOIN plants ON  plants.species_id = species.id 
# WHERE plants.age >= 10 AND plants.age <= 50 
# GROUP BY species.id;
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

# Aggregation function for max height.
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
for i in range(tracks):
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

result_stack = ResultFile(
    'results.txt',
    mux.output(),
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
    ('Mux', mux),
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

sys.stderr.write('%d,%d\n' % (tracks, len(threads)))
