from Queue import Queue
from threading import Lock, current_thread

from schema import Schema

class SortOrder(list):
    pass

class StreamClosedException(Exception):
    pass

class Stream(object):
    '''
    Representation of a record stream.
    '''
    class StreamEnd(object):
        pass

    class EndPoint(Queue):
        '''
        Representation of a stream's endpoint.
        '''
        def __init__(self):
            Queue.__init__(self, 1)
            self._queue = None
            self._recv_buffer = []
            self._send_buffer = []
            self._closed = False
            
        def receive(self, block = True):
            if not self._recv_buffer:
                self._recv_buffer = self.get(block)
                self._recv_buffer.reverse()
                self.task_done()

            o = self._recv_buffer.pop()
            if type(o) is Stream.StreamEnd:
                self._closed = True
                raise StreamClosedException
            else:
                return o

        def processed(self):
            # self.task_done()
            pass

        def close(self):
            self.send(Stream.StreamEnd(), True)

        def closed(self):
            return self._closed
 
        def send(self, o, flush = False):
            self._send_buffer.append(o)

            if len(self._send_buffer) >= 100 or flush:
                self.put(self._send_buffer)
                if self._queue:
                    self._queue.put(self)
                self._send_buffer = []

        def notify(self, queue):
            self._queue = queue

    def __init__(self, schema, sort_order, name = None):
        self._endpoints = list()
        self._schema = schema
        self._sort_order = sort_order
        self._name = name
            
    def connect(self):
        c = self.EndPoint()
        self._endpoints.append(c)
        return c

    def send(self, data):
        for c in self._endpoints:
            c.send(data)

    def close(self):
        for c in self._endpoints:
            c.close()

    def schema(self):
        return self._schema

    def sort_order(self):
        return self._sort_order

    def __repr__(self):
        if self._name:
            return '<Stream: \'%s\'>' % (self._name)
        else:
            return object.__repr__(self)

class Demux(object):
    class EndPoint(object):
        '''
        Representation of a stream's endpoint.
        '''
        def __init__(self, channel, idx):
            self._channel = channel
            self._i = idx
            self._queue = Queue()
            
        def receive(self, block = True):
            try:
                if self._queue.empty():
#                    print 'Updating for end point: [%d]' % (self._i)
                    self._channel._update(self._i)
            except StreamClosedException:
                if self._queue.empty():
                    raise
                else:
                    pass
#            print 'Returning for end point: [%d]' % (self._i)
            return self._queue.get()

        def processed(self):
            self._queue.task_done()

        def send(self, r):
            self._queue.put(r)
    
    class Channel(object):
        def __init__(self, demux):
            self._demux = demux
            self._ep = []
            self._lock = Lock()
            self._closed = False

        def schema(self):
            return self._demux.schema()

        def sort_order(self):
            return self._demux.sort_order()

        def connect(self):
            c = Demux.EndPoint(self, len(self._ep))
            self._ep.append(c)
            print 'Channel now has %d endpoints' % (len(self._ep))
            return c
        
        def _update(self, ep = -1):
#            print '\t%s: update called' % (ep)
            self._lock.acquire()
#            print '\t%s: lock acquired' % (ep) 
            if self._closed:
#                print '\t%s: stream closed' % (ep)
                self._lock.release()
#                print '\t%s: lock released' % (ep)
                raise StreamClosedException
            try:
#                print '\t%s: trying receive' % (ep)
                r = self._demux._receive()
#                print '\t%s: record received' % (ep)
                for e in self._ep:
#                    print '\t%s: sending to ep: %s' % (ep, e._i)
                    e.send(r)
            except StreamClosedException:
                self._closed = True
                raise
            finally:
                self._lock.release()


    def __init__(self, stream):
        self._stream = stream
        self._channels = []
        self._ep = self._stream.connect()
        self._lock = Lock()
        self._closed = False

    def schema(self):
        return self._stream.schema()

    def sort_order(self):
        return self._stream.sort_order()

    def send(self):
        raise Exception('send() is not implemented for a Demux stream.')

    def close(self):
        raise Exception('close() is not implemented for a Demux stream.')

    def __repr__(self):
        return '<Demux: %s >' % (self._stream)

    def channel(self):
        c = self.Channel(self)
        self._channels.append(c)
        return c

    def _receive(self, block = True):
        self._lock.acquire()
        if self._closed:
            self._lock.release()
            raise StreamClosedException
        try:
            return self._ep.receive(block)
        except StreamClosedException:
            self._closed = True
            raise
        finally:
            self._lock.release()
