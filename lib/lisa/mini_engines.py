import os

from threading import Event

from types import StopWord
from stream import Stream, SortOrder, StreamClosedException
from Queue import Queue, Empty
from schema import Schema

class MiniEngine(object):
    def __init__(self):
        # maybe setup some logging here
        pass

    def __call__(self):
        '''
        Main loop of the task which is called by the scheduler once to
        start the task.
        '''
        self.run()

    def output(self):
        '''
        Returns the output stream of the mini engine.
        '''
        return None

    def run(self):
        pass

class DataAccessor(MiniEngine):
    def __init__(self, query_stream, data_source, access_method):
        self._query_stream = query_stream
        
        # Create an accessor for the combination of access method and data
        # source. This should fail if access method and data source are not
        # compatible.
        self._accessor = access_method(data_source)
        self._data_source = data_source
        self._access_method = access_method

        # make sure the accessor understands the query schema
        assert self._accessor.accepts(self._query_stream.schema())
        self._query_stream_ep = self._query_stream.connect()

        # Create an output stream for this data accessor. The schema of the
        # output stream is determined by the data source.
        output_schema = self._data_source.schema()
    
        # We can only reasonably infer the sort order if all of the query
        # attributes are included in the output schema.
        if query_stream.sort_order() in output_schema:
            # The new sort order is the same as that of the query stream.
            sort_order = query_stream.sort_order()
        else:
            # No sort order can be inferred; using empty.
            sort_order = SortOrder()

        self._output_stream = Stream(
            output_schema, 
            sort_order, 
            'DATA ACCESSOR'
        )

    def output(self):
        '''
        Returns the output stream for the given data accessor.
        '''
        return self._output_stream;

    def run(self):
        '''
        The main loop of the data accessor mini-engine. It processes every
        element from the query stream and performs a corresponding query
        against the configured data source.
        '''
        # for each query in the query stream's end-point
        i = 0
        closed = False
        while not closed:
            try:
                # print 'DA: Waiting to receive'
                q = self._query_stream_ep.receive()
                i += 1
                if type(q) is StopWord:
                    continue
                if q:
                    # process the query and write result records into the output
                    # stream
                    for r in self._accessor.query(self._query_stream.schema(), q):
                        self._output_stream.send(r)
                    # finalize the partition that belongs to one query with a
                    # stop word
                    self._output_stream.send(StopWord())
                else:
                    # Technically stop words are not allowed in the query
                    # stream, but they are silently ignored here.
                    pass
                self._query_stream_ep.processed()
            except StreamClosedException:
                closed = True
        print 'Closing DA stream'
        self._output_stream.close()

class ArrayStreamer(MiniEngine):
    def __init__(self, schema, data, sort_order = None):
        MiniEngine.__init__(self)

        self._schema = schema
        self._data = data

        self._output_stream = Stream(
            self._schema, 
            sort_order or SortOrder(),
            'ARRAY STREAMER'
        )

    def output(self):
        return self._output_stream

    def run(self):
        for r in self._data:
            self._output_stream.send(r)
        print 'Closing ARRAY stream'
        self._output_stream.close()

class ResultStack(MiniEngine):
    def __init__(self, *streams):
        MiniEngine.__init__(self)
        self._streams = streams
        self._queue = Queue(1)
        self._endpoints = dict([(s.connect(), s) for s in self._streams])
        for e in self._endpoints:
            e.notify(self._queue)

    def run(self):
        while self._endpoints or not self._queue.empty():
            # print '\t\twaiting for endpoint'
            e = self._queue.get()
            
            if e in self._endpoints:
                # print 'got endpoint: %s' % (self._endpoints[e])
                pass
            else:
                # print 'got non-existing endpoint'
                continue

            valid = True
            closed = False
            while valid and not closed:
                # print '\t\t%s: receiving' % (self._endpoints[e])
                try:
                    r = e.receive(False)
                    print '\t\tReceived: %s from %s' % (r, self._endpoints[e])
                    e.processed()
                except StreamClosedException:
                    # print '\t\tReceive ClosedException.'
                    closed = True
                except:
                    valid = False
                    # print '\t\tReceive failed.'
            else:
                if e in self._endpoints:
                    # print '%s: closed? %s' % (self._endpoints[e], e.closed())
                    if closed:
                        # print '%s: closed? %s' % (self._endpoints[e], e.closed())
                        del self._endpoints[e]
                else:
                    # print '%s: already removed' % (e)
                    pass
            self._queue.task_done()
        print '\t\tAll streams done.'

class Select(MiniEngine):
    def __init__(self, input, transformer):
        MiniEngine.__init__(self)
        self._input = input
        self._input_ep = input.connect()
        self._t = transformer

        # make sure the transformer can handle the records in the stream
        assert self._t.accepts(input.schema())

        # We cannot reliably determine the sort order of the output stream
        # as the transformation applied to the attributes is unkown. If
        # for example the transformer inverts the value of an attribute the
        # attribute still has the same time and possibly name, but the
        # values in fact should now be sorted in reverse order.
        output_order = SortOrder()
        
        # Construct the output stream.
        self._output_stream = Stream(
            self._t.schema(), 
            output_order,
            'SELECT'
        )

    def output(self):
        return self._output_stream
    
    def run(self):
        closed = False
        while not closed:
            try:
                # print 'SELECT: waiting to receive'
                r = self._input_ep.receive()
                if type(r) is StopWord:
                    # print 'SELECT: got stop word'
                    self._output_stream.send(r)
                else:
                    # print 'SELECT: got record'
                    self._output_stream.send(self._t(r))
                self._input_ep.processed()
            except StreamClosedException:
                closed = True
        print 'Closing SELECT stream'
        self._output_stream.close()

class Mux(MiniEngine):
    def __init__(self, *streams):
        MiniEngine.__init__(self)
        if not streams:
            raise Exception('Mux: must specify at least one stream.')
        self._streams = streams
        self._queue = Queue(1)
        self._stats = {}
        self._endpoints = dict([(s.connect(), s) for s in self._streams])
        for e in self._endpoints:
            self._stats[e] = 0
            e.notify(self._queue)

        for s in self._streams:
            if s.schema() != self._streams[0].schema():
                raise Exception('Mux: schema of streams must match.')

        self._output = Stream(
            self._streams[0].schema(),
            SortOrder(),
            'Mux Output'
        )

    def output(self):
        return self._output

    def run(self):
        while self._endpoints or not self._queue.empty():
            # print '\t\twaiting for endpoint'
            e = self._queue.get()
            
            if e in self._endpoints:
                # print 'got endpoint: %s' % (self._endpoints[e])
                pass
            else:
                # print 'got non-existing endpoint'
                continue

            valid = True
            closed = False
            while valid and not closed:
                # print '\t\t%s: receiving' % (self._endpoints[e])
                try:
                    r = e.receive(False)
                    self._stats[e] += 1
                    self._output.send(r)
                    e.processed()
                except StreamClosedException:
                    # print '\t\tReceive ClosedException.'
                    closed = True
                except:
                    valid = False
                    # print '\t\tReceive failed.'
            else:
                if e in self._endpoints:
                    # print '%s: closed? %s' % (self._endpoints[e], e.closed())
                    if closed:
                        # print '%s: closed? %s' % (self._endpoints[e], e.closed())
                        del self._endpoints[e]
                else:
                    # print '%s: already removed' % (e)
                    pass
            self._queue.task_done()
        print '\t\tAll streams done.'
        self._output.close()
        for e in self._stats:
            print 'Received %d records from %s' % (self._stats[e], e)

class Group(MiniEngine):
    def __init__(self, input_stream, group_attributes):
        MiniEngine.__init__(self)
        self._input_stream = input_stream
        self._input_ep = input_stream.connect()
        self._schema = self._input_stream.schema()
        self._indices = {}
        for a in group_attributes:
            i = self._schema.index(a)
            t = self._schema[i].type()
            if group_attributes[a]:
                self._indices[i] = group_attributes[a]
            elif hasattr(t, '__eq__'):
                self._indices[i] = None
            else:
                raise Exception('Type of attribute [%s] does not have ' + \
                                'an equality operator.' % (a))

        self._output_stream = Stream(
            self._schema,
            self._input_stream.sort_order(), 
            'GROUP'
        )

    def output(self):
        return self._output_stream

    def _compare(self, a, b):
        for i in self._indices:
            if self._indices[i]:
                if not self._indices[i](a[i], b[i]):
                    return False
            else:
                if a[i] != b[i]:
                    return False
        return True

    def run(self):
        closed = False
        last = None
        while not closed:
            try:
                r = self._input_ep.receive()
                if type(r) is StopWord:
#                    print 'Sending: %s' % (str(StopWord()))
                    self._output_stream.send(StopWord())
#                    print 'Sending: %s' % (str(r))
                    self._output_stream.send(r)
                else:
                    if last == None or self._compare(last, r):
#                        print 'Sending: %s' % (str(r))
                        self._output_stream.send(r)
                    else:
#                        print 'Sending: %s' % (str(StopWord()))
                        self._output_stream.send(StopWord())
#                        print 'Sending: %s' % (str(r))
                        self._output_stream.send(r)
                    last = r
                self._input_ep.processed()
            except StreamClosedException:
                closed = True
        self._output_stream.send(StopWord())
        print 'Closing GROUP stream'
        self._output_stream.close()

class Sort(MiniEngine):
    def __init__(self, input_stream, sort_attributes, all = False):
        MiniEngine.__init__(self)
        self._input_stream = input_stream
        self._input_ep = input_stream.connect()
        self._schema = self._input_stream.schema()
        self._all = all
        self._indices = {}
        for a in sort_attributes:
            i = self._schema.index(a)
            t = self._schema[i].type()
            if sort_attributes[a]:
                self._indices[i] = sort_attributes[a]
            elif hasattr(t, '__cmp__'):
                self._indices[i] = None
            else:
                raise Exception('Type of attribute [%s] does not have ' + \
                                'a comparison operator.' % (a))

        self._output_stream = Stream(
            self._schema,
            SortOrder(), 
            'SORT'
        )

    def output(self):
        return self._output_stream

    def _compare(self, a, b):
        for i in self._indices:
            if self._indices[i]:
                x = self._indices[i](a[i], b[i])
                if x:
                    return x
            else:
                x = cmp(a[i], b[i])
                if x:
                    return x
        return 0

    def run(self):
        closed = False
        last = None
        set = []
        while not closed:
            try:
                r = self._input_ep.receive()
                if type(r) is StopWord:
                    if not self._all:
                        set.sort(self._compare)
                        for x in set:
                            self._output_stream.send(x)
                        set = []
                        self._output_stream.send(r)
                else:
                    set.append(r)
                self._input_ep.processed()
            except StreamClosedException:
                closed = True
        if self._all:
            set.sort(self._compare)
            for x in set:
                self._output_stream.send(x)
            self._output_stream.send(StopWord())
        print 'Closing SORT stream'
        self._output_stream.close()

class Join(MiniEngine):
    '''
    The Join mini-engine combines the records of two streams in such a way
    that the result is the Cartesian product between two corresponding
    record partitions, one from each stream.
    '''
    class PartitionBuffer(object):
        '''
        This class represents a partition buffer for a given endpoint. Each
        partition received from the endpoint is stored in a separate
        buffer.
        '''
        def __init__(self):
            self._b = []

        def append(self, r):
            if len(self._b) == 0:
                self._b.append([])

            self._b[-1].append(r)

        def current(self):
            return len(self._b) - 1

        def next(self):
            self._b.append([])

        def get(self, i):
            assert i >= 0 and i < len(self._b)
            return self._b[i]

        def finished(self, i):
            return i < len(self._b) - 1

        def remove(self, i):
            assert i >= 0 and i < len(self._b)
            t = self._b[i]
            self._b[i] = None
            del t

    def __init__(self, first, second):
        MiniEngine.__init__(self)

        self._first = first
        self._second = second

        # Construct the schema of the output stream.
        self._schema = Schema()
        for a in self._first.schema() + self._second.schema():
            self._schema.append(a)

        self._queue = Queue(1)
        
        self._first_ep = self._first.connect()
        self._first_ep.notify(self._queue)

        self._second_ep = self._second.connect()
        self._second_ep.notify(self._queue)

        self._output = Stream(
            self._schema,
            SortOrder(),
            'Join'
        )

        self._m = {
            self._first_ep: self._first,
            self._second_ep: self._second,
        }

    def output(self):
        return self._output

    def _merge(self, buffers, i):
        assert buffers[self._first_ep].finished(i)
        assert buffers[self._second_ep].finished(i)

        b1 = buffers[self._first_ep].get(i)
        b2 = buffers[self._second_ep].get(i)

        for r1 in b1:
            for r2 in b2:
                yield r1 + r2

        buffers[self._first_ep].remove(i)
        buffers[self._second_ep].remove(i)
        
    def run(self):
        done = False
        buffers = {
            self._first_ep: self.PartitionBuffer(),
            self._second_ep: self.PartitionBuffer(),
        }
        while not done or not self._queue.empty():
            e = self._queue.get()
            
            if e not in buffers:
                print 'ERROR: no buffer for endpoint'
                continue

            valid = True
            closed = False
            while valid and not closed:
                try:
                    r = e.receive(False)
                    if type(r) is not StopWord:
                        buffers[e].append(r)
                    else:
                        current = buffers[e].current()
                        buffers[e].next()
                        # Only merge if all buffers have completed this
                        # partition.
                        merge = True
                        for o in buffers:
                            merge &= buffers[o].finished(current)
                        if merge:
                            for x in self._merge(buffers, current):
                                self._output.send(x)
                            self._output.send(StopWord())

                        # Advance this buffer's partition by 1
                    e.processed()
                except StreamClosedException:
                    closed = True
                except Empty:
                    valid = False
                except:
                    raise
            else:
                done = True
                for o in buffers:
                    done &= o.closed()
            self._queue.task_done()
        self._output.close()

class Filter(MiniEngine):
    def __init__(self, input, predicate):
        MiniEngine.__init__(self)
        self._input = input
        self._predicate = predicate

        assert self._predicate.accepts(self._input.schema())

        self._input_ep = self._input.connect()

        self._output = Stream(
            self._input.schema(),
            self._input.sort_order(),
            'Filter'
        )

    def output(self):
        return self._output

    def run(self):
        closed = False
        while not closed:
            try:
                r = self._input_ep.receive()
                
                if type(r) is StopWord:
                    # Send if the record is a stop word
                    self._output.send(r)
                elif self._predicate(r):
                    # Send if the record satisfies the predicate.
                    self._output.send(r)
                self._input_ep.processed()
            except StreamClosedException:
                closed = True
        print 'Closing FILTER stream'
        self._output.close()

class Aggregate(MiniEngine):
    def __init__(self, input, aggregator):
        MiniEngine.__init__(self)
        self._input = input
        self._input_ep = self._input.connect()
        self._a = aggregator
        assert self._a.accepts(self._input.schema())

        self._output = Stream(
            self._input.schema(),
            self._input.sort_order(),
            'Aggregate'
        )

    def output(self):
        return self._output

    def run(self):
        closed = False

        # Initialize aggregate
        self._a.init()
        while not closed:
            try:
                r = self._input_ep.receive()
                if type(r) is StopWord:
                    # If the record is a stop word send the current
                    # aggregate value.
                    self._output.send(self._a.record())
                    # Additionally send the stop word
                    self._output.send(r)
                    # Reset the aggregate value
                    self._a.init()
                else:
                    # Add the current record to the aggregate
                    self._a(r)
                self._input_ep.processed()
            except StreamClosedException:
                closed = True
        print 'Closing AGGREGATE stream'
        self._output.close()

