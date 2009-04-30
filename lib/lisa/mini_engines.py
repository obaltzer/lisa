import os

from threading import Event

from types import StopWord
from stream import Stream, SortOrder, StreamClosedException
from Queue import Queue

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
                    # print '\t\tReceived: %s from %s' % (r, self._endpoints[e])
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
