import sys

sys.path.insert(0, 'lib')

from threading import Thread
import time

from lisa.schema import Schema, Attribute
from lisa.data_source import CSVFile
from lisa.access_methods import FindIdentities, FindRange
from lisa.types import Interval
from lisa.mini_engines import ArrayStreamer, DataAccessor, ResultStack, \
                              Select
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
        (IntInterval(1, 3),),
        (IntInterval(2, 5),),
        (IntInterval(2, 5),),
        (IntInterval(1, 3),),
        (IntInterval(2, 5),),
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

name_age_combiner = NameAgeCombiner(data_accessor.output().schema())

select = Select(data_accessor.output(), name_age_combiner)

name_age_combiner_reverse = NameAgeCombinerReverse(data_accessor.output().schema())

select2 = Select(data_accessor.output(), name_age_combiner_reverse)

result_stack = ResultStack(
    query_streamer.output(),
#    select.output(), 
#    select2.output(), 
#    data_accessor.output(),
)

def manage(task):
    print 'Running: ' + str(task)
    task.run()

t1 = Thread(
    target = manage, 
    name = 'Query Streamer', 
    args = (query_streamer,)
)
t2 = Thread(
    target = manage, 
    name = 'Data Accessor', 
    args = (data_accessor,)
)
t3 = Thread(
    target = manage, 
    name = 'Select', 
    args = (select,)
)
t4 = Thread(
    target = manage, 
    name = 'Result Stack', 
    args = (result_stack,)
)
t5 = Thread(
    target = manage, 
    name = 'Select2', 
    args = (select2,)
)

#t1.daemon = True
#t2.daemon = True
#t3.daemon = True
#t4.daemon = True

t1.start()
t2.start()
t3.start()
t4.start()
t5.start()

#time.sleep(1)

#t4 = stackless.tasklet(manage)(query_streamer)
#t2 = stackless.tasklet(manage)(data_accessor)
#t3 = stackless.tasklet(manage)(select)
#t1 = stackless.tasklet(manage)(result_stack)
#stackless.run()
