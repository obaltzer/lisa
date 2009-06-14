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
# Query 2
#
# SELECT family.id, genus.id, species.id, MAX(plants.height) 
# FROM family
# LEFT JOIN genus ON genus.family_id = family.id 
# LEFT JOIN species ON species.genus_id = genus.id 
# LEFT JOIN plants ON plants.species_id =  species.id 
# WHERE plants.age >= 10 AND plants.age <= 50 
# GROUP BY ROLLUP(family.id, genus.id, species.id)
#
#############################################################

# Schema definition of the query stream: an interval across all families.
query_schema = Schema()
query_schema.append(Attribute('family.id', IntInterval))

# Schema definition of the family record stream.
family_schema = Schema()
family_schema.append(Attribute('family.id', int))

# Schema definitions of the genus record stream.
genus_schema = Schema()
genus_schema.append(Attribute('genus.id', int))
genus_schema.append(Attribute('genus.family_id', int, True))

# Schema definitions of the species record stream.
species_schema = Schema()
species_schema.append(Attribute('species.id', int))
species_schema.append(Attribute('species.genus_id', int, True))

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

engines = []

# The query stream contains only a single query.
query_streamer = ArrayStreamer(query_schema, [
        (IntInterval(0, int(1E10)),),
])
engines.append(query_streamer)

# Create a family data source: a table in the input database.
family_source = DBTable(input_file, 'family', family_schema)
# Data accessor for the species data source.
family_accessor = DataAccessor(
    query_streamer.output(), 
    family_source,
    FindRange
)
engines.append(family_accessor)

# A group mini-engine to split the family IDs into groups.
family_id_grouper = Group(
    family_accessor.output(), 
    {'family.id': lambda a, b: a == b}
)
engines.append(family_id_grouper)

# Select only the family ID for querying genera.
family_id_select = Select(
    family_accessor.output(),
    UniversalSelect(
        family_accessor.output().schema(),
        {
            'genus.family_id': {
                'type': int,
                'args': ['family.id'],
                'function': lambda v: v
            }
        }
    )
)
engines.append(family_id_select)


# Data source for the genera.
genus_source = DBTable(input_file, 'genus', genus_schema)


# Data accessor for the genera data source.
genus_accessor = DataAccessor(
    family_id_select.output(), 
    genus_source,
    FindIdentities
)
engines.append(genus_accessor)


# A join mini-engine to associate families with genera.
family_genus_joiner = Join(
    family_id_grouper.output(), 
    genus_accessor.output(),
)
engines.append(family_genus_joiner)


# A group mini-engine to split the (family, genus) IDs into groups.
family_genus_id_grouper = Group(
    family_genus_joiner.output(), 
    {
        'family.id': lambda a, b: a == b,
        'genus.id': lambda a, b: a == b
    },
)
engines.append(family_genus_id_grouper)


# Select only the genus ID for querying species.
genus_id_select = Select(
    family_genus_joiner.output(),
    UniversalSelect(
        family_genus_joiner.output().schema(),
        {
            'species.genus_id': {
                'type': int,
                'args': ['genus.id'],
                'function': lambda v: v
            }
        }
    )
)
engines.append(genus_id_select)


# Data source for the species.
species_source = DBTable(input_file, 'species', species_schema)


# Data accessor for the species data source.
species_accessor = DataAccessor(
    genus_id_select.output(), 
    species_source,
    FindIdentities
)
engines.append(species_accessor)


# A join mini-engine to associate families, genera and species.
family_genus_species_joiner = Join(
    family_genus_id_grouper.output(), 
    species_accessor.output(),
)
engines.append(family_genus_species_joiner)

demux = Demux(family_genus_species_joiner.output())

mux_streams = []
for i in range(tracks):
    channel = demux.channel()

    # Select only the species ID for querying plants.
    species_id_select = Select(
        channel,
        UniversalSelect(
            channel.schema(),
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

    family_genus_species_id_grouper = Group(
        channel, 
        {
            'family.id': lambda a, b: a == b,
            'genus.id': lambda a, b: a == b,
            'species.id': lambda a, b: a == b
        }
    )
    engines.append(family_genus_species_id_grouper)
    # mux_streams.append(family_genus_species_id_grouper.output())

    species_plants_joiner = Join(
        family_genus_species_id_grouper.output(), 
        plants_height_aggregate.output()
    )
    engines.append(species_plants_joiner)
    mux_streams.append(species_plants_joiner.output())

mux = Mux(*mux_streams)
engines.append(mux)


# First aggregation level output selection
family_genus_species_select = Select(
    mux.output(),
    UniversalSelect(
        mux.output().schema(),
        [
            ('family.id', {
                'type': int,
                'args': ['family.id'],
                'function': lambda v: v
            }),
            ('genus.id', {
                'type': int,
                'args': ['genus.id'],
                'function': lambda v: v
            }),
            ('species.id', {
                'type': int,
                'args': ['species.id'],
                'function': lambda v: v
            }),
            ('plants.height', {
                'type': int,
                'args': ['plants.height'],
                'function': lambda v: v
            }),
        ]
    )
)
engines.append(family_genus_species_select)

# Second aggregation level output selection
family_genus_select = Select(
    family_genus_species_select.output(),
    UniversalSelect(
        family_genus_species_select.output().schema(),
        [
            ('family.id', {
                'type': int,
                'args': ['family.id'],
                'function': lambda v: v
            }),
            ('genus.id', {
                'type': int,
                'args': ['genus.id'],
                'function': lambda v: v
            }),
            ('plants.height', {
                'type': int,
                'args': ['plants.height'],
                'function': lambda v: v
            }),
        ]
    )
)
engines.append(family_genus_select)

# Generate aggregation groups for second aggregation level.
family_genus_grouper = Group(
    family_genus_select.output(), 
    {
        'family.id': lambda a, b: a == b,
        'genus.id': lambda a, b: a == b
    },
)
engines.append(family_genus_grouper)

# Aggregate second level
family_genus_aggregate = Aggregate(
    family_genus_grouper.output(),
    MaxHeightAggregator(family_genus_grouper.output().schema())
)
engines.append(family_genus_aggregate)

# Third aggregation level output selection
family_select = Select(
    family_genus_aggregate.output(),
    UniversalSelect(
        family_genus_aggregate.output().schema(),
        [
            ('family.id', {
                'type': int,
                'args': ['family.id'],
                'function': lambda v: v
            }),
            ('plants.height', {
                'type': int,
                'args': ['plants.height'],
                'function': lambda v: v
            }),
        ]
    )
)
engines.append(family_select)

# Generate aggregation groups for third aggregation level.
family_grouper = Group(
    family_select.output(), 
    {
        'family.id': lambda a, b: a == b,
    },
)
engines.append(family_grouper)

# Aggregate third level
family_aggregate = Aggregate(
    family_grouper.output(),
    MaxHeightAggregator(family_grouper.output().schema())
)
engines.append(family_aggregate)

# Fourth aggregation level output selection
all_select = Select(
    family_aggregate.output(),
    UniversalSelect(
        family_aggregate.output().schema(),
        {
            'plants.height': {
                'type': int,
                'args': ['plants.height'],
                'function': lambda v: v
            },
        }
    )
)
engines.append(all_select)

# Generate aggregation groups for fourth aggregation level.
all_grouper = Group(
    all_select.output(), 
    {
    },
)
engines.append(all_grouper)

# Aggregate fourth level
all_aggregate = Aggregate(
    all_grouper.output(),
    MaxHeightAggregator(all_grouper.output().schema())
)
engines.append(all_aggregate)

result_stack = ResultFile(
    'results.txt',
    family_genus_species_select.output(),
#    family_genus_aggregate.output(),
#    family_aggregate.output(),
#    all_aggregate.output(),
)
engines.append(result_stack)

info_queue = Queue()

def manage(task):
    task.run()
    info_queue.put((task, ThreadInfo()))

tasks = []
tasks += [(e.name, e) for e in engines]

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

infos = {}
while not info_queue.empty():
    t, i = info_queue.get()
    infos[t] = i
    info_queue.task_done()

for name, task in tasks:
    print infos[task]

sys.stderr.write('%d,%d\n' % (tracks, len(threads)))
