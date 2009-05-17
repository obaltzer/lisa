import sys

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
                              Aggregate
from lisa.stream import Demux
from lisa.util import UniversalSelect
from lisa.info import ThreadInfo

query = Geometry(Polygon((
    (-93.88, 49.81), 
    (-65.39, 49.81), 
    (-65.39, 24.22),
    (-93.88, 24.22)
)).wkb)

counties_file = 'data/spatial/counties'
geonames_file = 'data/spatial/geonames_small'

#############################################################
#
# Query 3
#
# SELECT counties.id, COUNT(geonames.*) FROM counties
# LEFT JOIN geonames ON CONTAINS(counties.the_geom, geonames.location)
# WHERE 
#   CONTAINS(
#       MakeBox2D(
#           MakePoint(-93.88, 49.81),
#           MakePoint(-65.39, 24.22)
#       ),
#       geonames.location
#   )
# GROUP BY counties.id;
#
#############################################################

# Schema definition of the query stream: an interval across all counties.
query_schema = Schema()
query_schema.append(Attribute('counties.the_geom', Geometry))

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

engines = []

# The query stream contains only a single query box.
query_streamer = ArrayStreamer(query_schema, [
        (query,),
])
engines.append(query_streamer)

counties_source = Rtree(counties_file, 'counties.the_geom')

counties_accessor = DataAccessor(
    query_streamer.output(),
    counties_source,
    FindRange,
)
engines.append(counties_accessor)

demux = Demux(counties_accessor.output())

mux_streams = []
for i in range(4):
    channel = demux.channel()
    
    # To query the locations in the geonames layer, trim the counties to
    # the query.
    counties_select = Select(
        channel,
        UniversalSelect(
            channel.schema(),
            {
                'geonames.location': {
                    'type': Geometry,
                    'args': ['counties.the_geom'],
                    'function': lambda v: Geometry(v.geom().intersection(query.geom()).wkb),
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

    geonames_aggregate = Aggregate(
        geonames_select.output(),
        SumAggregator(geonames_select.output().schema(), 'count')
    )
    engines.append(geonames_aggregate)

    counties_grouper = Group(
        channel, 
        {'oid': lambda a, b: a == b}
    )
    engines.append(counties_grouper)

    joiner = Join(counties_grouper.output(), geonames_aggregate.output())
    engines.append(joiner)
    mux_streams.append(joiner.output())

mux = Mux(*mux_streams)
engines.append(mux)

result_stack = ResultStack(
    mux.output(),
)
engines.append(result_stack)

info_queue = Queue()

def manage(task):
    task.run()
    info_queue.put(ThreadInfo())

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
