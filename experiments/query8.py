# Same as query 6 except tracks are split before the customer layer (2nd
# level).

import sys
import logging
import time
import os

# Setup the package search path.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))

from multiprocessing import Process
from multiprocessing import current_process

from multiprocessing import JoinableQueue as Queue

from lisa.schema import Schema, Attribute
from lisa.data_source import DBTable
from lisa.access_methods import FindIdentities, FindRange
from lisa.types import IntInterval
from lisa.mini_engines import ArrayStreamer, DataAccessor, ResultStack, \
                              Select, Mux, Group, Join, Filter, \
                              Aggregate, ResultFile, Demux, Sort
from lisa.util import UniversalSelect

tracks = int(sys.argv[1])
input_file = sys.argv[2]

log = logging.getLogger()
log.setLevel(logging.INFO)
ch = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
log.addHandler(ch)

#############################################################
#
# Query 2
#
# SELECT nation.id, customer.id, orders.id, MAX(lineitem.price) 
# FROM nation
# LEFT JOIN customer ON customer.nation_id = nation.id 
# LEFT JOIN orders ON orders.customer_id = customer.id 
# LEFT JOIN lineitem ON lineitem.order_id =  orders.id 
# WHERE lineitem.quantity >= 10 AND lineitem.quantity <= 15
# GROUP BY ROLLUP(nation.id, customer.id, orders.id)
#
#############################################################

# Schema definition of the query stream: an interval across all families.
query_schema = Schema()
query_schema.append(Attribute('nation.id', IntInterval))

# Schema definition of the nation record stream.
nation_schema = Schema()
nation_schema.append(Attribute('nation.id', int))

# Schema definitions of the customer record stream.
customer_schema = Schema()
customer_schema.append(Attribute('customer.id', int))
customer_schema.append(Attribute('customer.nation_id', int, True))

# Schema definitions of the orders record stream.
orders_schema = Schema()
orders_schema.append(Attribute('orders.id', int))
orders_schema.append(Attribute('orders.customer_id', int, True))

# Schema definition of the lineitem record stream.
lineitem_schema = Schema()
# lineitem_schema.append(Attribute('lineitem.id', int))
lineitem_schema.append(Attribute('lineitem.quantity', int))
lineitem_schema.append(Attribute('lineitem.price', float))
lineitem_schema.append(Attribute('lineitem.order_id', int, True))

# Filter lineitem to only include those 10 years or older and 50 years or
# younger.
class FilterQuantity(object):
    def __init__(self, input_schema):
        self._input_schema = input_schema
        self._p = [
            (
                input_schema.index('lineitem.quantity'), 
                lambda x: x >= 10 and x <= 15
            ),
        ]

    def accepts(self, other_schema):
        return self._input_schema == other_schema

    def __call__(self, r):
        for p in self._p:
            if not p[1](r[p[0]]):
                return False
        return True

# Aggregation function for max price.
class MaxPriceAggregator(object):
    def __init__(self, input_schema):
        self._input_schema = input_schema
        self._af = []
        for a in self._input_schema:
            if a.name() == 'lineitem.price':
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

# Create a nation data source: a table in the input database.
nation_source = DBTable(input_file, 'nation', nation_schema)
# Data accessor for the orders data source.
nation_accessor = DataAccessor(
    query_streamer.output(), 
    nation_source,
    FindRange
)
engines.append(nation_accessor)

demux0 = Demux(nation_accessor.output())
engines.append(demux0)

mux0_streams = []
for i in range(tracks / 2 + 1):
    
    channel0 = demux0.channel()

    # A group mini-engine to split the nation IDs into groups.
    nation_id_grouper = Group(
        channel0, 
        {'nation.id': lambda a, b: a == b}
    )
    engines.append(nation_id_grouper)

    # Select only the nation ID for querying genera.
    nation_id_select = Select(
        channel0,
        UniversalSelect(
            channel0.schema(),
            {
                'customer.nation_id': {
                    'type': int,
                    'args': ['nation.id'],
                    'function': lambda v: v
                }
            }
        )
    )
    engines.append(nation_id_select)


    # Data source for the genera.
    customer_source = DBTable(input_file, 'customer', customer_schema)


    # Data accessor for the genera data source.
    customer_accessor = DataAccessor(
        nation_id_select.output(), 
        customer_source,
        FindIdentities
    )
    engines.append(customer_accessor)

    # A join mini-engine to associate families with genera.
    nation_customer_joiner = Join(
        nation_id_grouper.output(), 
        customer_accessor.output(),
    )
    engines.append(nation_customer_joiner)

    demux = Demux(nation_customer_joiner.output())
    engines.append(demux)

    mux_streams = []
    for j in range(tracks):
        channel = demux.channel()
     
        # A group mini-engine to split the (nation, customer) IDs into groups.
        nation_customer_id_grouper = Group(
            channel, 
            {
                'nation.id': lambda a, b: a == b,
                'customer.id': lambda a, b: a == b
            },
        )
        engines.append(nation_customer_id_grouper)


        # Select only the customer ID for querying orders.
        customer_id_select = Select(
            channel,
            UniversalSelect(
                nation_customer_joiner.output().schema(),
                {
                    'orders.customer_id': {
                        'type': int,
                        'args': ['customer.id'],
                        'function': lambda v: v
                    }
                }
            )
        )
        engines.append(customer_id_select)


        # Data source for the orders.
        orders_source = DBTable(input_file, 'orders', orders_schema)


        # Data accessor for the orders data source.
        orders_accessor = DataAccessor(
            customer_id_select.output(), 
            orders_source,
            FindIdentities
        )
        engines.append(orders_accessor)

        # A join mini-engine to associate families, genera and orders.
        nation_customer_orders_joiner = Join(
            nation_customer_id_grouper.output(), 
            orders_accessor.output(),
        )
        engines.append(nation_customer_orders_joiner)

        # Select only the orders ID for querying lineitem.
        orders_id_select = Select(
            nation_customer_orders_joiner.output(),
            UniversalSelect(
                nation_customer_orders_joiner.output().schema(),
                {
                    'lineitem.order_id': {
                        'type': int,
                        'args': ['orders.id'],
                        'function': lambda v: v
                    }
                }
            )
        )
        engines.append(orders_id_select)

        # Data source for the lineitem.
        lineitem_source = DBTable(input_file, 'lineitem', lineitem_schema)
        # Data accessor for the lineitem data source.
        lineitem_accessor = DataAccessor(
            orders_id_select.output(), 
            lineitem_source,
            FindIdentities
        )
        engines.append(lineitem_accessor)

        lineitem_filter = Filter(
            lineitem_accessor.output(),
            FilterQuantity(lineitem_accessor.output().schema())
        )
        engines.append(lineitem_filter)

        # Select only the orders ID for querying lineitem.
        lineitem_price_select = Select(
            lineitem_filter.output(),
            UniversalSelect(
                lineitem_filter.output().schema(),
                {
                    'lineitem.price': {
                        'type': int,
                        'args': ['lineitem.price'],
                        'function': lambda v: v
                    }
                }
            )
        )
        engines.append(lineitem_price_select)

        lineitem_price_aggregate = Aggregate(
            lineitem_price_select.output(),
            MaxPriceAggregator(lineitem_price_select.output().schema())
        )
        engines.append(lineitem_price_aggregate)

        nation_customer_orders_id_grouper = Group(
            nation_customer_orders_joiner.output(), 
            {
                'nation.id': lambda a, b: a == b,
                'customer.id': lambda a, b: a == b,
                'orders.id': lambda a, b: a == b
            }
        )
        engines.append(nation_customer_orders_id_grouper)
        # mux_streams.append(nation_customer_orders_id_grouper.output())

        orders_lineitem_joiner = Join(
            nation_customer_orders_id_grouper.output(), 
            lineitem_price_aggregate.output()
        )
        engines.append(orders_lineitem_joiner)
        mux_streams.append(orders_lineitem_joiner.output())

    mux = Mux(*mux_streams)
    engines.append(mux)

    mux0_streams.append(mux.output())

mux0 = Mux(*mux0_streams)
engines.append(mux0)

# First aggregation level output selection
nation_customer_orders_select = Select(
    mux0.output(),
    UniversalSelect(
        mux0.output().schema(),
        [
            ('nation.id', {
                'type': int,
                'args': ['nation.id'],
                'function': lambda v: v
            }),
            ('customer.id', {
                'type': int,
                'args': ['customer.id'],
                'function': lambda v: v
            }),
            ('orders.id', {
                'type': int,
                'args': ['orders.id'],
                'function': lambda v: v
            }),
            ('lineitem.price', {
                'type': int,
                'args': ['lineitem.price'],
                'function': lambda v: v
            }),
        ]
    )
)
engines.append(nation_customer_orders_select)

# Second aggregation level output selection
nation_customer_select = Select(
    nation_customer_orders_select.output(),
    UniversalSelect(
        nation_customer_orders_select.output().schema(),
        [
            ('nation.id', {
                'type': int,
                'args': ['nation.id'],
                'function': lambda v: v
            }),
            ('customer.id', {
                'type': int,
                'args': ['customer.id'],
                'function': lambda v: v
            }),
            ('lineitem.price', {
                'type': int,
                'args': ['lineitem.price'],
                'function': lambda v: v
            }),
        ]
    )
)
engines.append(nation_customer_select)

nation_customer_sorter = Sort(
    nation_customer_select.output(),
    [  
        ('nation.id', lambda a, b: cmp(a, b)),
        ('customer.id', lambda a, b: cmp(a, b))
    ],
    True # sort all input, not only the current partition
)
engines.append(nation_customer_sorter)

# Generate aggregation groups for second aggregation level.
nation_customer_grouper = Group(
    nation_customer_sorter.output(), 
    {
        'nation.id': lambda a, b: a == b,
        'customer.id': lambda a, b: a == b
    },
)
engines.append(nation_customer_grouper)

# Aggregate second level
nation_customer_aggregate = Aggregate(
    nation_customer_grouper.output(),
    MaxPriceAggregator(nation_customer_grouper.output().schema())
)
engines.append(nation_customer_aggregate)

# Third aggregation level output selection
nation_select = Select(
    nation_customer_aggregate.output(),
    UniversalSelect(
        nation_customer_aggregate.output().schema(),
        [
            ('nation.id', {
                'type': int,
                'args': ['nation.id'],
                'function': lambda v: v
            }),
            ('lineitem.price', {
                'type': int,
                'args': ['lineitem.price'],
                'function': lambda v: v
            }),
        ]
    )
)
engines.append(nation_select)

# Generate aggregation groups for third aggregation level.
nation_grouper = Group(
    nation_select.output(), 
    {
        'nation.id': lambda a, b: a == b,
    },
)
engines.append(nation_grouper)

# Aggregate third level
nation_aggregate = Aggregate(
    nation_grouper.output(),
    MaxPriceAggregator(nation_grouper.output().schema())
)
engines.append(nation_aggregate)

# Fourth aggregation level output selection
all_select = Select(
    nation_aggregate.output(),
    UniversalSelect(
        nation_aggregate.output().schema(),
        {
            'lineitem.price': {
                'type': float,
                'args': ['lineitem.price'],
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
    MaxPriceAggregator(all_grouper.output().schema())
)
engines.append(all_aggregate)

result_file = ResultFile(
    'query6-results.txt',
    nation_customer_orders_select.output(),
    nation_customer_aggregate.output(),
    nation_aggregate.output(),
    all_aggregate.output(),
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
