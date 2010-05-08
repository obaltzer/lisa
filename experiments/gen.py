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
                              Generator, Demux
from lisa.util import UniversalSelect
from lisa.info import ThreadInfo
from types import IntType

log = logging.getLogger()
log.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(name)-20s: %(levelname)-8s %(message)s')
console = logging.StreamHandler()
console.setFormatter(formatter)
console.setLevel(logging.DEBUG)
log.addHandler(console)

# Schema definition of the input stream
input_schema = Schema()
input_schema.append(Attribute('number', IntType))

engines = []

input_streamer = Generator(input_schema, xrange(0, 10000))
engines.append(input_streamer)

#for i in xrange(10):
#    result_stack = ResultStack(
#        input_streamer.output(),
#    )
#    engines.append(result_stack)

demux = Demux(input_streamer.output())
engines.append(demux)
mux_streams = []
for i in range(20):
    channel = demux.channel()

    input_select = Select(
        channel,
        UniversalSelect(
            input_streamer.output().schema(),
            {
                'mult': {
                    'type': IntType,
                    'args': ['number'],
                    'function': lambda v: v * v,
                },
            },
        )
    )
    engines.append(input_select)
    mux_streams.append(input_select.output())

#    result_stack = ResultStack(
#        input_select.output(),
#    )
#    engines.append(result_stack)
  
mux = Mux(*mux_streams)
engines.append(mux)

#input_streams = []
#for i in xrange(0, 20):
#    input_streamer = Generator(input_schema, xrange(0, 10000))
#    engines.append(input_streamer)
#    input_select = Select(
#        input_streamer.output(),
#        UniversalSelect(
#            input_streamer.output().schema(),
#            {
#                'mult': {
#                    'type': IntType,
#                    'args': ['number'],
#                    'function': lambda v: v * v,
#                },
#            },
#        )
#    )
#    engines.append(input_select)
#    input_streams.append(input_select.output())

#mux = Mux(*input_streams)
#engines.append(mux)

result_stack = ResultStack(
    mux.output(),
)
engines.append(result_stack)

def manage(name, task):
    print '%s: %s' % (name, str(current_process().pid))
    task.run()
    # info_queue.put((task, ThreadInfo()))

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
    log.info('Waiting for %s' % (t))
    t.join()
    log.info('Done %s' % (t))

log.info('All threads are done.')

infos = {}
#while not info_queue.empty():
#    t, i = info_queue.get()
#    infos[t] = i
#    info_queue.task_done()

#for name, task in tasks:
#    print infos[task]

# sys.stderr.write('%d,%d\n' % (tracks, len(threads)))
