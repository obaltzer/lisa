import sys
import logging
import time
import os

# Setup the package search path.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))

from multiprocessing import Process
from multiprocessing import current_process

from multiprocessing import JoinableQueue as Queue
from shapely.geometry import Polygon

from lisa.schema import Schema, Attribute
from lisa.data_source import Rtree
from lisa.access_methods import FindIdentities, FindRange
from lisa.types import IntInterval, Geometry, StopWord
from lisa.mini_engines import ArrayStreamer, DataAccessor, ResultStack, \
                              Select, Mux, Group, Join, Filter, \
                              Aggregate, ResultFile, Counter, Sort, \
                              Demux, Limit
from lisa.util import UniversalSelect
from lisa.info import ThreadInfo

tracks = int(sys.argv[1])

log = logging.getLogger()
log.setLevel(logging.INFO)
ch = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
log.addHandler(ch)

query = Geometry(Polygon((
    (-93.88, 49.81), 
    (-65.39, 49.81), 
    (-65.39, 24.22),
    (-93.88, 24.22)
)))

counties_file = sys.argv[2]
#geonames_file = 'data/spatial/geonames_medium'
geonames_file = sys.argv[3]

#############################################################
#
# Query 3
#
# SELECT counties.id, COUNT(geonames.*) FROM counties
# LEFT JOIN geonames ON CONTAINS(counties.geom, geonames.location)
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
query_schema.append(Attribute('counties.geom', Geometry))

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

counties_source = Rtree(counties_file, 'counties.geom')
counties_accessor = DataAccessor(
    query_streamer.output(),
    counties_source,
    FindRange,
)
engines.append(counties_accessor)

demux = Demux(counties_accessor.output())
engines.append(demux)

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

mux_streams = []
for i in range(tracks):
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
                    'args': ['counties.geom'],
                    'function': lambda v: intersection(v, query),
                }
            }
        )
    )
    engines.append(counties_select)

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

    select = Select(
        channel,
        UniversalSelect(
            channel.schema(),
            {
                'oid': {
                    'type': int,
                    'args': ['oid'],
                    'function': lambda v: v
                },
            }
        )
    )
    engines.append(select)

    counties_grouper = Group(
        select.output(), 
        {'oid': lambda a, b: a == b}
    )
    engines.append(counties_grouper)

    joiner = Join(counties_grouper.output(), geonames_aggregate.output())
    engines.append(joiner)
    mux_streams.append(joiner.output())

mux = Mux(*mux_streams)
engines.append(mux)

result_file = ResultFile(
    'query3-results.txt',
    mux.output(),
)
engines.append(result_file)

def manage(name, task):
    print '%s: %s' % (name, str(current_process().pid))
    task.run()

tasks = []
tasks += [(e.name, e) for e in engines]

threads = []
for t in tasks:
    threads.append(
        Process(
            target = manage, 
            name = t[0], 
            args = (t[0], t[1],)
        )
    )

for t in threads:
    t.start()

for t in threads:
    t.join()
    log.info('Done %s' % (t))

log.info('All threads are done.')

sys.stderr.write('%d,%d\n' % (tracks, len(threads)))
