# vim: wrap

import os

from types import StopWord
from stream import Stream, SortOrder, StreamClosedException, \
                   end_point_counter, EndPoint as BasicEndPoint
from multiprocessing.queues import JoinableQueue as Queue
from multiprocessing import current_process
from Queue import Empty
from schema import Schema


import logging

class MiniEngine(object):
    def __init__(self):
        self.name = str(self.__class__.__name__)
        self.log = logging.getLogger(self.name)
        print 'Creating ME: %s' % (self.name)
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
        MiniEngine.__init__(self)
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
        self._output_stream.close()
        print 'Closing DA stream'

class Generator(MiniEngine):
    def __init__(self, schema, generator, sort_order = None):
        MiniEngine.__init__(self)

        self._schema = schema
        self._generator = generator

        self._output_stream = Stream(
            self._schema,
            sort_order or SortOrder(),
            'GENERATOR'
        )

    def output(self):
        return self._output_stream

    def run(self):
        log = logging.getLogger('Generator %s' % (current_process().pid))
        for o in self._generator:
            self._output_stream.send((o,))
        self._output_stream.close()
        log.info('Closing generator stream.')

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
        self._queue = Queue(100)
        self.log = logging.getLogger('ResultStack')
        self.log.info('Output schemas:')
        for s in self._streams:
            self.log.info(', '.join([a.name() for a in s.schema()]))
        self._ep_st = dict([(s.connect(), s) for s in self._streams])
        self._ep = dict([(ep.name(), ep) for ep in self._ep_st.keys()])
        for e in self._ep.values():
            e.notify(self._queue)

    def run(self):
        c = 0
        while self._ep or not self._queue.empty():
           
            self.log.debug('Waiting endpoint for notify.')
            e = self._queue.get()
            
            if e in self._ep:
                self.log.debug('got endpoint: %s' % (e))
                pass
            else:
                self.log.debug('got non-existing endpoint')
                continue

            valid = True
            closed = False
            # Block until the element is available on the endpoint. We know
            # the element should be available shortly since we already
            # received a notification about it.
            if not closed:
                self.log.debug('%s: receiving' % (e))
                try:
                    # r = self._ep[e].receive(False)
                    # This is the blocking read from the end point.
                    r = self._ep[e].receive(True)
                    if type(r) is not StopWord:
                        self.log.debug('Received: %s from %s' % (r, e))
                        c += 1
                    else:
                        self.log.debug('STOP')
                        pass
                    self._ep[e].processed()

                except StreamClosedException:
                    self.log.debug('Receive ClosedException.')
                    closed = True
                except:
                    valid = False
                    self.log.debug('Receive failed.')
            
            if closed:
                if e in self._ep:
                    self.log.debug('%s: closed? %s' % (
                        e,
                        self._ep[e].closed()
                    ))
                    if closed:
                        self.log.debug('%s: closed? %s' % (
                            e, 
                            self._ep[e].closed()
                        ))
                        del self._ep[e]
                else:
                    self.log.debug('%s: already removed' % (e))
                    pass
            # self._queue.task_done()
        self.log.info('All streams done.')
        self.log.info('Received %d record.' % (c))
        self.log.info('ResultStack: done.')

class ResultFile(MiniEngine):
    def __init__(self, outfile, *streams):
        MiniEngine.__init__(self)
        if outfile:
            self._f = open(outfile, 'w')
        else:
            import sys
            self._f = sys.stdout
        self._streams = streams
        self._queue = Queue(100)
        # print 'Output schemas:'
        for s in self._streams:
            print ', '.join([a.name() for a in s.schema()])
        self._ep_st = dict([(s.connect(), s) for s in self._streams])
        self._ep = dict([(ep.name(), ep) for ep in self._ep_st])
        for e in self._ep.values():
            e.notify(self._queue)

    def run(self):
        c = 0
        while self._ep or not self._queue.empty():
            # print '\t\twaiting for endpoint'
            e = self._queue.get()
            
            if e in self._ep:
                # print 'got endpoint: %s' % (self._endpoints[e])
                pass
            else:
                # print 'got non-existing endpoint'
                continue

            valid = True
            closed = False
            if not closed:
                self.log.debug('%s: receiving' % (e))
                try:
                    r = self._ep[e].receive(True)
                    if type(r) is not StopWord:
                        o = [
                            type(x) is float and ('%.8e' % x) or \
                            str(x)
                            for x in r
                        ]
                        self._f.write(','.join(o) + '\n')
                        c += 1
                    else:
                        # self._f.write('--STOP--\n')
                        pass
                    self._ep[e].processed()
                except StreamClosedException:
                    self.log.debug('Received closed exception.')
                    closed = True
                except:
                    valid = False
                    self.log.debug('Receive failed.')
            
            if closed:
                if e in self._ep:
                    # print '%s: closed? %s' % (self._endpoints[e], e.closed())
                    if closed:
                        # print '%s: closed? %s' % (self._endpoints[e], e.closed())
                        del self._ep[e]
                else:
                    # print '%s: already removed' % (e)
                    pass
            # self._queue.task_done()
        # print '\t\tAll streams done.'
        # print '\t\tResultFile: Received %d record.' % (c)
        self._f.close()
        print '\t\tResultFile: done. Write %d record.' % (c)

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
        self._output_stream.close()
        print 'Closing SELECT stream'

class Mux(MiniEngine):
    def __init__(self, *streams):
        MiniEngine.__init__(self)
        if not streams:
            raise Exception('Mux: must specify at least one stream.')
        self._streams = streams
        self._queue = Queue(100)
        self._stats = {}
        self._ep_st = dict([(s.connect(), s) for s in self._streams])
        self._ep = dict([(ep.name(), ep) for ep in self._ep_st.keys()])
        for e in self._ep:
            self._stats[e] = 0
            self._ep[e].notify(self._queue)

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
        while self._ep or not self._queue.empty():
            # print '\t\twaiting for endpoint'
            ep_name = self._queue.get()
            
            if ep_name not in self._ep:
                print '\t********* got non-existing endpoint'
                continue
            
            e = self._ep[ep_name]
            valid = True
            closed = False
            while valid and not closed:
                # print '\t\t%s: receiving' % (self._endpoints[e])
                try:
                    # r = self._ep[e].receive(False)
                    # We just received notification, so lets keep blocking
                    # until we have the element.
                    r = e.receive(block = True)
                    self._stats[ep_name] += 1
                    self._output.send(r)
                    e.processed()

                    # Only receive one message at a time.
                    valid = False
                except StreamClosedException:
                    # print '\t\tReceive ClosedException.'
                    closed = True
                except Empty:
                    #valid = False
                    # Keep trying if the receive failed.
                    valid = True
                    # print '\t\tReceive failed.'
            else:
                if ep_name in self._ep:
                    # print '%s: closed? %s' % (self._endpoints[e], e.closed())
                    if closed:
                        # print '%s: closed? %s' % (self._endpoints[e], e.closed())
                        del self._ep[ep_name]
                else:
                    # print '%s: already removed' % (e)
                    pass
            # self._queue.task_done()
        #print '\t\tAll streams done.'
        self._output.close()
        # for e in self._stats:
        #    print 'Received %d records from %s' % (self._stats[e], e)
        print 'Mux: done.'

class Group(MiniEngine):
    def __init__(self, input_stream, group_attributes):
        MiniEngine.__init__(self)
        self._input_stream = input_stream
        self._input_ep = input_stream.connect()
        self._schema = self._input_stream.schema()
        print self._schema
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
                    #self._output_stream.send(StopWord())
#                    print 'Sending: %s' % (str(r))
                    #self._output_stream.send(r)
                    pass
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
        self._indices = []
        # Passed as attribues = [('name', comparison_function), ...]
        for a in sort_attributes:
            # Check if the given attribute exists in the schema.
            i = self._schema.index(a[0])
            t = self._schema[i].type()

            if a[1]:
                # If a comparison function is specified, use it.
                self._indices.append((i, a[1]))
            elif hasattr(t, '__cmp__'):
                # Otherwise test if the given type has a comparator and use
                # it.
                self._indices.append((i, None))
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
            # Defined as i = (index, comparator)
            if i[1]:
                x = i[1](a[i[0]], b[i[0]])
                if x != 0:
                    return x
            else:
                x = cmp(a[i[0]], b[i[0]])
                if x != 0:
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

        self._queue = Queue(100)
        
        self._first_ep = self._first.connect()
        self._first_ep.notify(self._queue)

        self._second_ep = self._second.connect()
        self._second_ep.notify(self._queue)

        self._ep = {
            self._first_ep.name(): self._first_ep,
            self._second_ep.name(): self._second_ep,
        }
        
        self._output = Stream(
            self._schema,
            SortOrder(),
            'Join'
        )

        self._m = {
            self._first_ep: self._first,
            self._second_ep: self._second,
        }

        self._empty = 0

    def output(self):
        return self._output

    def _merge(self, buffers, i):
        assert buffers[self._first_ep].finished(i)
        assert buffers[self._second_ep].finished(i)

        b1 = buffers[self._first_ep].get(i)
        b2 = buffers[self._second_ep].get(i)

        if len(b2) == 1:
            self._empty += 1

        for r1 in b1[:-1]:
            for r2 in b2[:-1]:
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
            ep_name = self._queue.get()
            # print 'Join: got event for (%s)' % (ep_name)
            e = self._ep[ep_name]
            
            if e not in buffers:
                print 'ERROR: no buffer for endpoint'
                continue

            valid = True
            closed = False
            while valid and not closed:
                try:
                    # r = e.receive(False)
                    # We got notification, so assume we will receive
                    # something here.
                    r = e.receive(block = True)
                    buffers[e].append(r)
                    if type(r) is StopWord:
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

                    # let's make sure we only process one element
                    valid = False
                except StreamClosedException:
                    closed = True
                except Empty:
                    # valid = False
                    # keep busy waiting until we have processed at least one
                    # element
                    valid = True
                except:
                    raise
            else:
                done = True
                for o in buffers:
                    done &= o.closed()
            # self._queue.task_done()
        self._output.close()
        print 'Join done. %d empty buffers.' % (self._empty)

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
                    # aggregate value if anything was aggregated.
                    if self._a.count():
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

class Counter(MiniEngine):
    def __init__(self, input):
        MiniEngine.__init__(self)
        self._input = input
        self._input_ep = self._input.connect()
        self._s = 0
        self._r = 0

    def stats(self):
        return (self._r, self._s)

    def run(self):
        closed = False
        while not closed:
            try:
                r = self._input_ep.receive()
                if type(r) is StopWord:
                    self._s += 1
                else:
                    self._r += 1
                self._input_ep.processed()
            except StreamClosedException:
                closed = True
        print 'Closing COUNTER: %d record, %d stop words' % \
                (self._r, self._s)

class Limit(MiniEngine):
    '''
    The Limit mini-engine limits the number of records that are being
    passed through it. It sends a StreamEnd message as soon as the limit is
    reached and discards all subsequent records it receives until the
    record source is depleted.
    '''
    def __init__(self, input, limit):
        '''
        If the specified limit is less than 0 not limit on the number of
        records will be imposed.
        '''
        super(self.__class__, self).__init__()
        self._input = input
        self._input_end_point = self._input.connect()
        self._limit = limit
        
        # The output stream into which the records will be written.
        self._output_stream = Stream(
            self._input.schema(),
            self._input.sort_order(),
            'LIMIT'
        )
        self.log.info('Initialization completed.')

    def output(self):
        return self._output_stream

    def run(self):
        # Number of records processed so far
        count = 0
        closed = False
        output_done = False
        while not closed:
            try:
                r = self._input_end_point.receive()
                if self._limit < 0 or count < self._limit:
                    self._output_stream.send(r)
                    count += 1

                self._input_end_point.processed()
            except StreamClosedException:
                closed = True

            if not output_done and (count == self._limit or closed):
                # close the output stream
                self.log.debug(
                    'Closing output stream after %d recrods.' % (count)
                )
                self._output_stream.close()
                output_done = True

        self.log.info('Processing completed.')

class Channel(object):
    '''
    The Channel class encapsulates a single outgoing channel from the
    Demux mini-engine. As such it transports a different
    (complimentary) set of message compared to its sibling channels.
    Each channel may have multiple consumers
    '''
    

    class EndPoint(BasicEndPoint):
        '''
        An endpoint is a connection between a channel's source process
        and one of the channel's consumer processes. Each channel may
        have any number of endpoints. This endpoint implementation is
        an extension to the basic endpoint implementation provided by
        the stream implementation.
        '''
        def __init__(self, channel):
            # Initialize the BasicEndPoint with a queue size of 10
            super(self.__class__, self).__init__(1)
            # Remember what channel we are associated with
            self._channel = channel

        def receive(self, block = True):
            '''
            Overwrite the BasicEndPoint's implementation of receive()
            with one that also notifies the end point's channel that
            the message has been received.
            '''
            try:
                return super(self.__class__, self).receive(block)
            finally:
                # We do not need to notify if the endpoint is already
                # closed
                if not self._closed:
                    self._channel.notify_demux()
            
    def __init__(self, demux, id):
        self._demux = demux
        self._id = id
        self._end_points = []
        # This member holds the number of outstanding notification the
        # channel needs to receive before it can be considered as free.
        self._outstanding = 0
        self._closed = False

    def connect(self):
        '''
        This method returns an endpoint that is being used by a
        consumer process. It should consist of a queue, that is being
        used by channel to send elements to the attached process.
        
        Executed in the Setup process.
        '''
        end_point = self.EndPoint(self)
        self._end_points.append(end_point)
        return end_point

    def notify_demux(self):
        '''
        This method is called by the attached end-point when the
        receiving process has consumed the element. It connects to the
        Demux's engines notification queue and places this channels
        reference (by ID) into the queue.
        
        Executed in the Consumer process.
        '''
        self._demux.notify_received_message(self._id)

    def notify(self):
        '''
        This method is called by the Demux mini-engine to decrement
        this channel's outstanding notifications counter. Only if the
        outstanding notifications counter is 0 the channel is
        considered to be free.

        Executed in the Demux process.
        '''
        self._outstanding -= 1

    def free(self):
        '''
        The free() method returns True if this channel no longer has
        any outstanding notifications and thus can accept new messages.
        
        Executed in the Demux process.
        '''
        return self._outstanding == 0

    def send(self, m):
        '''
        Sends the specified message to all end-points associated with
        this channel and resets the notification counter to the number
        of endpoints attached to the channel.

        Executed in the Demux process.
        '''
        assert(m != None)
        self._outstanding = len(self._end_points)
        for end_point in self._end_points:
            end_point.send(m)

    def close(self):
        '''
        Closes the channel. This method is called by the Demux instance
        that owns this channel and causes the channel to call the
        close() method on each of the end points associated with it.
        '''
        for end_point in self._end_points:
            end_point.close()
        self._closed = True

    def schema(self):
        '''
        Returns the schema of the Demux instance.
        '''
        return self._demux.schema()

    def sort_order(self):
        '''
        Returns the sort order of the Demux instance.
        '''
        return self._demux.sort_order()

class Demux(MiniEngine):
    '''
    The Demux mini-engine can be used to distribute (demultiplex) the
    elements of a single incoming stream to multiple outgoing streams.

    The process is implemented as follows:

    1. The Demux waits for an element to arrive at the incoming end-point.

    2. The Demux queries the internal non-shared queue for a channel that
       is empty.

    3. The Demux places the element into the channel. The channel in turn
       iterates over its end-points an places the element into the
       corresponding queues.

    4. The Demux repeats step 1-3 for until there are no more empty
       channels are available.

    5. The Demux queries the channel end-point notification queue for
       process completion notifications. For each received notification the
       Demux decrements the notification counter associated with the
       corresponding channel. If the channel becomes free the Demux
       continues with step 1.

    6. Once a stream end exception was received by the Demux engine, the
       same message is sent to all channels as they are become free.

    ------------------------------------------------------------------------

    1. A channel consumer waits for an element to appear at its end-point.
       Immediately of the element has been received the consumer signals
       the channel (indirectly the Demux) that it has received the element.

    2. See step 5 above.
    '''

    def __init__(self, input):
        MiniEngine.__init__(self)
        self._input = input
        # The connection endpoint to the input stream
        self._input_end_point = self._input.connect()
        # The list of channels handled by the demux
        self._channels = []
        self._free_channels = []
        # The queue of notifications that were received from the end points
        # that are connected to each channel. All of these messages are
        # kept in a single queue to avoid that we block when checking on
        # one channel which other channels could already be processed.
        self._received_messages = Queue(100)
        self.log.debug('Initialization complete.')

    def schema(self):
        return self._input.schema()

    def sort_order(self):
        return self._input.sort_order()

    def channel(self):
        '''
        The channel() method should return a stream object which allows the
        consumer to setup a IPC queue between the Demux mini-engine and
        itself.

        This method is executed in the setup process.
        '''
        c = Channel(self, len(self._channels))
        self._channels.append(c)
        self._free_channels.append(c)
        return c

    def notify_received_message(self, channel):
        '''
        This method is called to notify the Demux process of a message that
        was received by a channel's end point. We collect all these
        notifications in a single queue to avoid blocking on some channels
        while other channels can already be processed.

        This method is executed in the message consumer process.
        '''
        self._received_messages.put(channel)

    def run(self):
        closed = False
        while not closed:
            try:
                self.log.debug('Waiting for input message.')
                m = self._input_end_point.receive()
                self.log.debug('Input message received.')
                # Get a free channel
                c = self._free_channels.pop(0)
                self.log.debug('Got free channel [%s].' % (c))
                # Write message to the channel
                assert(m != None)
                c.send(m)
                self.log.debug('Delivered message to channel [%s].' % (c))
                self._input_end_point.processed()

                # Check if there are more free channels, if not wait for
                # a channel to become available.
                while not self._free_channels:
                    self.log.debug('Waiting for channel to become free.')
                    c = self._received_messages.get()
                    self.log.debug('Channel[%s] received a message.' % (c))
                    self._channels[c].notify()
                    if self._channels[c].free():
                        self.log.debug(
                            'Channel[%s] has become free.' % (c)
                        )
                        self._free_channels.append(self._channels[c])
                    # self._received_messages.task_done()
            except StreamClosedException:
                closed = True
        
        self.log.debug('Input stream is complete.')
        # The input stream is done. Now wait until all channels report back
        # that they are free and all messages have been processed.
        self.log.debug('Waiting for all channels to become free.')
        self.log.debug('Total Channels: %d, Free Channels: %d' % (
            len(self._channels),
            len(self._free_channels)
        ))

        while len(self._free_channels) != len(self._channels) or \
                not self._received_messages.empty():
            c = self._received_messages.get()
            self._channels[c].notify()
            if self._channels[c].free():
                self._free_channels.append(self._channels[c])
        
        self.log.debug('Total Channels: %d, Free Channels: %d' % (
            len(self._channels),
            len(self._free_channels)
        ))

        self.log.debug('Sending closing instruction to each channel.')
        # Close each channel.
        for c in self._channels:
            c.close()

        self.log.info('Process completed.')
