import sys
import logging
import time

# Setup the package search path.
sys.path.insert(0, '../lib')

from threading import Thread, current_thread
from Queue import Queue
from shapely.geometry import Polygon

from lisa.schema import Schema, Attribute
from lisa.data_source import Rtree
from lisa.access_methods import FindIdentities, FindRange
from lisa.types import IntInterval, Geometry
from lisa.mini_engines import ArrayStreamer, DataAccessor, ResultStack, \
                              Select, Mux, Group, Join, Filter, \
                              Aggregate, ResultFile, Counter, Sort
from lisa.stream import Demux
from lisa.util import UniversalSelect
from lisa.info import ThreadInfo

tracks = int(sys.argv[1])

log = logging.getLogger('main')
log.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
log.addHandler(ch)

query = Geometry(Polygon((
    (-93.88, 49.81), 
    (-65.39, 49.81), 
    (-65.39, 24.22),
    (-93.88, 24.22)
)))


states_file = 'data/spatial/states'
counties_file = 'data/spatial/counties'
geonames_file = 'data/spatial/geonames_medium'
#geonames_file = 'data/spatial/geonames'

#############################################################
#
# Query 4
#
# SELECT us_states.gid, us_counties.gid, COUNT(geonames.*) FROM us_states
# LEFT JOIN us_counties 
#   ON CONTAINS(us_states.the_geom, us_counties.the_geom)
# LEFT JOIN geonames ON CONTAINS(us_counties.the_geom, geonames.location)
# WHERE 
#   CONTAINS(
#       MakeBox2D(
#           MakePoint(-93.88, 49.81),
#           MakePoint(-65.39, 24.22)
#       ),
#       geonames.location
#   )
# GROUP BY ROLLUP(us_states.gid, us_counties.gid);
#
#############################################################

# Schema definition of the query stream: an interval across all states.
query_schema = Schema()
query_schema.append(Attribute('states.the_geom', Geometry))

# Aggregation function for max height.
class SumAggregator(object):
    def __init__(self, input_schema, f):
        self._input_schema = input_schema
        self._af = []
        for a in self._input_schema:
            if a.name() == f:
                # Only keep the maximum
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

# Helper function to compute the intersection between two geometries.
def intersection(a, b):
    g1 = a.geom()
    g2 = b.geom()
    try:
        if g1.is_valid and g2.is_valid:
            i = g1.intersection(g2)
            return Geometry(i)
        else:
            return None
    except:
        return None

engines = []
counters = []

# The query stream contains only a single query box.
query_streamer = ArrayStreamer(query_schema, [
        (query,),
])
engines.append(query_streamer)

# Query the states from the data source.
states_source = Rtree(states_file, 'states.the_geom')
states_accessor = DataAccessor(
    query_streamer.output(),
    states_source,
    FindRange
)
engines.append(states_accessor)

# Trim the states to the query region.
states_select = Select(
    states_accessor.output(),
    UniversalSelect(
        states_accessor.output().schema(),
        {
            # trim geometry
            'states.the_geom': {
                'type': Geometry,
                'args': ['states.the_geom'],
                'function': lambda v: intersection(v, query),
            },
            # keep OID
            'states.oid': {
                'type': int,
                'args': ['oid'],
                'function': lambda v: v,
            }
        }
    )
)
engines.append(states_select)

# Only keep the geometry for querying
states_query = Select(
    states_select.output(),
    UniversalSelect(
        states_select.output().schema(),
        {
            'counties.the_geom': {
                'type': Geometry,
                'args': ['states.the_geom'],
                'function': lambda v: v,
            },
        }
    )
)
engines.append(states_query)

# Finally query the counties
counties_source = Rtree(counties_file, 'counties.the_geom')
counties_accessor = DataAccessor(
    states_query.output(),
    counties_source,
    FindRange,
)
engines.append(counties_accessor)

# Rename the OID attribute of the counties
counties_oid_select = Select(
    counties_accessor.output(),
    UniversalSelect(
        counties_accessor.output().schema(),
        {
            'counties.oid': {
                'type': int,
                'args': ['oid'],
                'function': lambda v: v,
            },
            'counties.the_geom': {
                'type': Geometry,
                'args': ['counties.the_geom'],
                'function': lambda v: v,
            },
        }
    )
)
engines.append(counties_oid_select)

# Group states by OID
states_group = Group(
    states_select.output(), 
    {'states.oid': lambda a, b: a == b}
)
engines.append(states_group)

# Join counties and states
states_counties_join = Join(
    states_group.output(),
    counties_oid_select.output(),
)
engines.append(states_counties_join)

# De-multiplex the joined stream across multiple tracks for better CPU core
# utilization.
demux = Demux(states_counties_join.output())
mux_streams = []
for i in range(tracks):
    channel = demux.channel()
    
    # To query the locations in the geonames layer, trim the counties to
    # the state and query boundary.
    counties_select = Select(
        channel,
        UniversalSelect(
            channel.schema(),
            {
                'geonames.location': {
                    'type': Geometry,
                    'args': ['states.the_geom', 'counties.the_geom'],
                    'function': lambda s, c: intersection(s, c),
                }
            }
        )
    )
    engines.append(counties_select)

    # Data source for geonames
    geonames_source = Rtree(geonames_file, 'geonames.location')
    # Data accessor for the geonames.
    geonames_accessor = DataAccessor(
        counties_select.output(), 
        geonames_source,
        FindRange
    )
    engines.append(geonames_accessor)

    # XXX At this point no additional filter for the contraining the
    # geonames to the query region is required.
    
    # Send '1' for each retrieved geoname location.
    geonames_select = Select(
        geonames_accessor.output(),
        UniversalSelect(
            geonames_accessor.output().schema(),
            {
                'count': {
                    'type': int,
                    'args': ['geonames.location'],
                    'function': lambda v: 1
                }
            }
        )
    )
    engines.append(geonames_select)

    # Aggregate the geonames
    geonames_aggregate = Aggregate(
        geonames_select.output(),
        SumAggregator(geonames_select.output().schema(), 'count')
    )
    engines.append(geonames_aggregate)

    # Select only the OIDs from each of the hierarchy levels.
    select = Select(
        channel,
        UniversalSelect(
            channel.schema(),
            {
                'states.oid': {
                    'type': int,
                    'args': ['states.oid'],
                    'function': lambda v: v
                },
                'counties.oid': {
                    'type': int,
                    'args': ['counties.oid'],
                    'function': lambda v: v
                },
            }
        )
    )
    engines.append(select)
   
    # Generate appropriate groups
    states_counties_grouper = Group(
        select.output(), 
        {
            'states.oid': lambda a, b: a == b,
            'counties.oid': lambda a, b: a == b
        }
    )
    engines.append(states_counties_grouper)

    joiner = Join(
        states_counties_grouper.output(), 
        geonames_aggregate.output()
    )
    engines.append(joiner)
    mux_streams.append(joiner.output())

mux = Mux(*mux_streams)
engines.append(mux)

states_level_select = Select(
    mux.output(),
    UniversalSelect(
        mux.output().schema(),
        {
            'states.oid': {
                'type': int,
                'args': ['states.oid'],
                'function': lambda v: v,
            },
            'count': {
                'type': int,
                'args': ['count'],
                'function': lambda v: v,
            }
        }
    )
)
engines.append(states_level_select)

states_ungroup = Group(
    states_level_select.output(),
    {
    }
)
engines.append(states_ungroup)

states_sort = Sort(
    states_ungroup.output(),
    [
        ('states.oid', None)
    ]
)
engines.append(states_sort)

states_level_group = Group(
    states_sort.output(),
    {
        'states.oid': lambda a, b: a == b,
    }
)
engines.append(states_level_group)

# Aggregate second level
states_level_aggregate = Aggregate(
    states_level_group.output(),
    SumAggregator(states_level_group.output().schema(), 'count')
)
engines.append(states_level_aggregate)

all_level_select = Select(
    states_level_aggregate.output(),
    UniversalSelect(
        states_level_aggregate.output().schema(),
        {
            'count': {
                'type': int,
                'args': ['count'],
                'function': lambda v: v,
            }
        }
    )
)
engines.append(all_level_select)

all_group = Group(
    all_level_select.output(),
    {
    }
)
engines.append(all_group)

# Aggregate third level
all_level_aggregate = Aggregate(
    all_group.output(),
    SumAggregator(all_level_select.output().schema(), 'count')
)
engines.append(all_level_aggregate)

output_attr = Select(
    states_counties_join.output(),
    UniversalSelect(
        states_counties_join.output().schema(),
        {
            'states.oid': {
                'type': int,
                'args': ['states.oid'],
                'function': lambda v: v,
            },
            'counties.oid': {
                'type': int,
                'args': ['counties.oid'],
                'function': lambda v: v,
            }
        }
    )
)
engines.append(output_attr)

# Output
result_stack = ResultFile(
    'results.txt',
    mux.output(),
    states_level_aggregate.output(),
    all_level_aggregate.output()
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

log.info('Waiting for threads.')

for t in threads:
    t.join()
    log.info('Done %s' % (t))

log.info('All threads are done.')

for c in counters:
    print 'Counter: %d records, %d stop words' % c.stats()

infos = {}
while not info_queue.empty():
    t, i = info_queue.get()
    infos[t] = i
    info_queue.task_done()

for name, task in tasks:
    print infos[task]

sys.stderr.write('%d,%d\n' % (tracks, len(threads)))
