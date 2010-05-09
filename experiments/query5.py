import sys
import os
import time
import os

# Setup the package search path.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))

from multiprocessing import Process
from multiprocessing import current_process

from multiprocessing import Queue
from shapely.geometry import Polygon

from lisa.schema import Schema, Attribute
from lisa.data_source import Rtree
from lisa.access_methods import FindIdentities, FindRange
from lisa.types import IntInterval, Geometry, StopWord
from lisa.mini_engines import ArrayStreamer, DataAccessor, ResultStack, \
                              Select, Mux, Group, Join, Filter, \
                              Aggregate, ResultFile, Counter, Sort
from lisa.stream import Demux
from lisa.util import UniversalSelect
from lisa.info import ThreadInfo

tracks = int(sys.argv[1])

#query = Geometry(Polygon((
#    (-93.88, 49.81), 
#    (-65.39, 49.81), 
#    (-65.39, 24.22),
#    (-93.88, 24.22)
#)))

tl, br = sys.argv[2].split(':')
tx, ty = [float(x) for x in tl.split(',')]
bx, by = [float(x) for x in br.split(',')]

#query = Geometry(Polygon((
#    (-93.88, 49.81), 
#    (-65.39, 49.81), 
#    (-65.39, 24.22),
#    (-93.88, 24.22)
#)))

query = Geometry(Polygon((
    (tx, ty),
    (bx, ty),
    (bx, by),
    (tx, by)
)))

states_file = sys.argv[3]
counties_file = sys.argv[4]
zip_file = sys.argv[5]
cover_file = sys.argv[6]

#states_file = 'data/spatial/states'
#counties_file = 'data/spatial/counties'
#zip_file = 'data/spatial/zip5'
#cover_file = 'data/spatial/' + sys.argv[2]

#############################################################
#
# Query 5
#
#############################################################

# Schema definition of the query stream.
query_schema = Schema()
query_schema.append(Attribute('queries.geom', Geometry))

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

class UniversalFilter(object):
    def __init__(self, input_schema, filters):
        self._input_schema = input_schema
        self._p = []
        for k in filters:
            self._p.append((input_schema.index(k), filters[k]))

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
# Query
#
#############################################################

engines = []

# The query stream contains only a single query box.
query_streamer = ArrayStreamer(query_schema, [
        (query,),
        StopWord(),
])
engines.append(query_streamer)

#############################################################
#
# States
#
#############################################################

states_query = Select(
    query_streamer.output(),
    UniversalSelect(
        query_streamer.output().schema(),
        {
            'states.geom': {
                'type': Geometry,
                'args': ['queries.geom'],
                'function': lambda v: v,
            },
        }
    )
)
engines.append(states_query)

states_source = Rtree(states_file, 'states.geom')
states_accessor = DataAccessor(
    states_query.output(),
    states_source,
    FindRange
)
engines.append(states_accessor)

states_select = Select(
    states_accessor.output(),
    UniversalSelect(
        states_accessor.output().schema(),
        {
            'states.oid': {
                'type': int,
                'args': ['oid'],
                'function': lambda v: v,
            },
            'states.geom': {
                'type': Geometry,
                'args': ['states.geom'],
                'function': lambda v: v,
            }
        }
    )
)
engines.append(states_select)

states_join = Join(
    query_streamer.output(),
    states_select.output()
)
engines.append(states_join)

states_trim = Select(
    states_join.output(),
    UniversalSelect(
        states_join.output().schema(),
        {
            'states.oid': {
                'type': int,
                'args': ['states.oid'],
                'function': lambda v: v,
            },
            'states.geom': {
                'type': Geometry,
                'args': ['queries.geom', 'states.geom'],
                'function': lambda a, b: intersection(a, b),
            }
        }
    )
)
engines.append(states_trim)

states_group = Group(
    states_trim.output(),
    {
        'states.oid': lambda a, b: a == b,
    }
)
engines.append(states_group)

#############################################################
#
# Counties
#
#############################################################

counties_query = Select(
    states_trim.output(),
    UniversalSelect(
        states_trim.output().schema(),
        {
            'counties.geom': {
                'type': Geometry,
                'args': ['states.geom'],
                'function': lambda v: v,
            }
        }
    )
)
engines.append(counties_query)

counties_source = Rtree(counties_file, 'counties.geom')
counties_accessor = DataAccessor(
    counties_query.output(),
    counties_source,
    FindRange,
)
engines.append(counties_accessor)

counties_select = Select(
    counties_accessor.output(),
    UniversalSelect(
        counties_accessor.output().schema(),
        {
            'counties.oid': {
                'type': int,
                'args': ['oid'],
                'function': lambda v: v,
            },
            'counties.geom': {
                'type': Geometry,
                'args': ['counties.geom'],
                'function': lambda v: v,
            }
        }
    )
)
engines.append(counties_select)

counties_join = Join(
    states_group.output(),
    counties_select.output()
)
engines.append(counties_join)

counties_trim = Select(
    counties_join.output(),
    UniversalSelect(
        counties_join.output().schema(),
        {
            'states.oid': {
                'type': int,
                'args': ['states.oid'],
                'function': lambda v: v,
            },
            'states.geom': {
                'type': Geometry,
                'args': ['states.geom'],
                'function': lambda v: v,
            },
            'counties.oid': {
                'type': int,
                'args': ['counties.oid'],
                'function': lambda v: v,
            },
            'counties.geom': {
                'type': Geometry,
                'args': ['states.geom', 'counties.geom'],
                'function': lambda a, b: intersection(a, b),
            }
        }
    )
)
engines.append(counties_trim)

counties_filter = Filter(
    counties_trim.output(),
    UniversalFilter(
        counties_trim.output().schema(),
        {
            'counties.geom': lambda g: g and g.geom().is_valid and g.geom().area != 0,
        }
    )
)
engines.append(counties_filter)

counties_group = Group(
    counties_filter.output(),
    {
        'states.oid': lambda a, b: a == b,
        'counties.oid': lambda a, b: a == b,
    }
)
engines.append(counties_group)

#############################################################
#
# Zip
#
#############################################################

zip_query = Select(
    counties_filter.output(),
    UniversalSelect(
        counties_filter.output().schema(),
        {
            'zip.geom': {
                'type': Geometry,
                'args': ['counties.geom'],
                'function': lambda v: v,
            }
        }
    )
)
engines.append(zip_query)

zip_source = Rtree(zip_file, 'zip.geom')
zip_accessor = DataAccessor(
    zip_query.output(),
    zip_source,
    FindRange,
)
engines.append(zip_accessor)

zip_select = Select(
    zip_accessor.output(),
    UniversalSelect(
        zip_accessor.output().schema(),
        {
            'zip.oid': {
                'type': int,
                'args': ['oid'],
                'function': lambda v: v,
            },
            'zip.geom': {
                'type': Geometry,
                'args': ['zip.geom'],
                'function': lambda v: v,
            }
        }
    )
)
engines.append(zip_select)

zip_join = Join(
    counties_group.output(),
    zip_select.output()
)
engines.append(zip_join)

zip_trim = Select(
    zip_join.output(),
    UniversalSelect(
        zip_join.output().schema(),
        {
            'states.oid': {
                'type': int,
                'args': ['states.oid'],
                'function': lambda v: v,
            },
            'states.geom': {
                'type': Geometry,
                'args': ['states.geom'],
                'function': lambda v: v,
            },
            'counties.oid': {
                'type': int,
                'args': ['counties.oid'],
                'function': lambda v: v,
            },
            'counties.geom': {
                'type': Geometry,
                'args': ['counties.geom'],
                'function': lambda v: v,
            },
            'zip.oid': {
                'type': int,
                'args': ['zip.oid'],
                'function': lambda v: v,
            },
            'zip.geom': {
                'type': Geometry,
                'args': ['counties.geom', 'zip.geom'],
                'function': lambda a, b: intersection(a, b),
            }
        }
    )
)
engines.append(zip_trim)

zip_filter = Filter(
    zip_trim.output(),
    UniversalFilter(
        zip_trim.output().schema(),
        {
            'zip.geom': lambda g: g and g.geom().is_valid and g.geom().area != 0,
        }
    )
)
engines.append(zip_filter)

demux = Demux(zip_filter.output())

mux_streams = []
for i in range(tracks):
    channel = demux.channel()
    
    zip_group = Group(
        channel,
        {
            'states.oid': lambda a, b: a == b,
            'counties.oid': lambda a, b: a == b,
            'zip.oid': lambda a, b: a == b,
        }
    )
    engines.append(zip_group)
    
    cover_query = Select(
        channel,
        UniversalSelect(
            channel.schema(),
            {
                'cover.geom': {
                    'type': Geometry,
                    'args': ['zip.geom'],
                    'function': lambda v: v,
                }
            }
        )
    )
    engines.append(cover_query)

    cover_source = Rtree(cover_file, 'cover.geom')
    cover_accessor = DataAccessor(
        cover_query.output(), 
        cover_source,
        FindRange
    )
    engines.append(cover_accessor)

    cover_select = Select(
        cover_accessor.output(),
        UniversalSelect(
            cover_accessor.output().schema(),
            {
                'cover.geom': {
                    'type': Geometry,
                    'args': ['cover.geom'],
                    'function': lambda v: v,
                }
            }
        )
    )
    engines.append(cover_select)
    
    cover_join = Join(
        zip_group.output(),
        cover_select.output(),
    )
    engines.append(cover_join)

    cover_area = Select(
        cover_join.output(),
        UniversalSelect(
            cover_join.output().schema(),
            [
                ('states.oid', {
                    'type': int,
                    'args': ['states.oid'],
                    'function': lambda v: v,
                }),
                #'states.geom': {
                #    'type': Geometry,
                #    'args': ['states.geom'],
                #    'function': lambda v: v,
                #},
                ('counties.oid', {
                    'type': int,
                    'args': ['counties.oid'],
                    'function': lambda v: v,
                }),
                #'counties.geom': {
                #    'type': Geometry,
                #    'args': ['counties.geom'],
                #    'function': lambda v: v,
                #},
                ('zip.oid', {
                    'type': int,
                    'args': ['zip.oid'],
                    'function': lambda v: v,
                }),
                #'zip.geom': {
                #    'type': Geometry,
                #    'args': ['counties.geom', 'zip.geom'],
                #    'function': lambda a, b: intersection(a, b),
                #},
                ('area', {
                    'type': float,
                    'args': ['zip.geom', 'cover.geom'],
                    'function': 
                        lambda a, b: 
                            intersection(a, b).geom().area / b.geom().area
                })
            ]
        )
    )
    engines.append(cover_area)

#############################################################
#
# 1st level aggregation
#
#############################################################

    cover_aggregate = Aggregate(
        cover_area.output(),
        SumAggregator(cover_area.output().schema(), 'area')
    )
    engines.append(cover_aggregate)
    mux_streams.append(cover_aggregate.output())

mux = Mux(*mux_streams)
engines.append(mux)

#############################################################
#
# 2nd level aggregation
#
#############################################################

counties_level_select = Select(
    mux.output(),
    UniversalSelect(
        mux.output().schema(),
        [
            ('states.oid', {
                'type': int,
                'args': ['states.oid'],
                'function': lambda v: v,
            }),
            ('counties.oid', {
                'type': int,
                'args': ['counties.oid'],
                'function': lambda v: v,
            }),
            ('area', {
                'type': float,
                'args': ['area'],
                'function': lambda v: v,
            }),
        ]
    )
)
engines.append(counties_level_select)

counties_level_ungroup = Group(
    counties_level_select.output(),
    {}
)
engines.append(counties_level_ungroup)

counties_level_sort = Sort(
    counties_level_ungroup.output(),
    [
        ('states.oid', None),
        ('counties.oid', None)
    ]
)
engines.append(counties_level_sort)

counties_level_group = Group(
    counties_level_sort.output(),
    {
        'states.oid': lambda a, b: a == b,
        'counties.oid': lambda a, b: a == b,
    }
)
engines.append(counties_level_group)

counties_level_aggregate = Aggregate(
    counties_level_group.output(),
    SumAggregator(counties_level_group.output().schema(), 'area')
)
engines.append(counties_level_aggregate)

#############################################################
#
# 3rd level aggregation
#
#############################################################

states_level_select = Select(
    counties_level_aggregate.output(),
    UniversalSelect(
        counties_level_aggregate.output().schema(),
        [
            ('states.oid', {
                'type': int,
                'args': ['states.oid'],
                'function': lambda v: v,
            }),
            ('area', {
                'type': float,
                'args': ['area'],
                'function': lambda v: v,
            }),
        ]
    )
)
engines.append(states_level_select)

states_level_ungroup = Group(
    states_level_select.output(),
    {}
)
engines.append(states_level_ungroup)

states_level_sort = Sort(
    states_level_ungroup.output(),
    [
        ('states.oid', None)
    ]
)
engines.append(states_level_sort)

states_level_group = Group(
    states_level_sort.output(),
    {
        'states.oid': lambda a, b: a == b,
    }
)
engines.append(states_level_group)

states_level_aggregate = Aggregate(
    states_level_group.output(),
    SumAggregator(states_level_group.output().schema(), 'area')
)
engines.append(states_level_aggregate)

#############################################################
#
# 4th level aggregation
#
#############################################################

# all_level_select = Select(
#     counties_level_aggregate.output(),
#     UniversalSelect(
#         counties_level_aggregate.output().schema(),
#         {
#             'area': {
#                 'type': float,
#                 'args': ['area'],
#                 'function': lambda v: v,
#             },
#         }
#     )
# )
# engines.append(all_level_select)
# 
# all_level_group = Group(
#     all_level_select.output(),
#     {}
# )
# engines.append(all_level_group)
# 
# all_level_aggregate = Aggregate(
#     all_level_group.output(),
#     SumAggregator(all_level_group.output().schema(), 'area')
# )
# engines.append(all_level_aggregate)

#############################################################
#
#  Output
#
#############################################################

result_stack = ResultFile(
    'results.txt',
    mux.output(),
    counties_level_aggregate.output(),
    states_level_aggregate.output(),
#    all_level_aggregate.output()
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
        Process(
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
