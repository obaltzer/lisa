from multiprocessing.queues import JoinableQueue as Queue
from Queue import Empty
from multiprocessing import Lock, current_process

from schema import Schema

def end_point_counter_closure():
    k = 0
    while True:
        k += 1
        yield k

end_point_counter = end_point_counter_closure().next

class SortOrder(list):
    pass

class StreamClosedException(Exception):
    pass

class StreamEnd(object):
    pass

class EndPoint(Queue):
    '''
    Representation of a stream's endpoint.
    '''
    def __init__(self):
        Queue.__init__(self, 1)
        self._closed = False
        self._queue = None
        self._name = end_point_counter()

    def name(self):
        return self._name
        
    def receive(self, block = True):
        try:
            #print 'EndPoint[%04d : %d(%s)]: get enter' % (
            #    self._name,
            #    current_process().pid, 
            #    str(current_process().name),
            #)
            o = self.get(block)
            self.task_done()
            # print 'EndPoint[%04d : %d(%s)]: get exit (%s)' % (
            #    self._name, 
            #    current_process().pid, 
            #    str(current_process().name),
            #    str(o)
            #)
        
            if type(o) is StreamEnd:
                #print 'EndPoint[%04d : %d(%s)]: closing stream' % (
                #    self._name, 
                #    current_process().pid,
                #    str(current_process().name),
                #)
                self._closed = True
                raise StreamClosedException
            else:
                return o
        except Empty:
            #print 'EndPoint[%04d : %d(%s)]: get exit (EMPTY)' % (
            #    self._name, 
            #    current_process().pid, 
            #    str(current_process().name),
            #)
            raise

    def processed(self):
        # self.task_done()
        pass

    def close(self):
        self.send(StreamEnd(), True)

    def closed(self):
        return self._closed

    def send(self, o, flush = False):
        #print 'EndPoint[%04d : %d]: put enter (%s)' % (self._name, current_process().pid, str(o))
        self.put(o)
        #print 'EndPoint[%04d : %d]: put exit' % (self._name, current_process().pid)
        if self._queue:
            #print 'EndPoint[%04d : %d]: notify put enter' % (self._name, current_process().pid)
            self._queue.put(self._name)
            #print 'EndPoint[%04d : %d]: notify put exit' % (self._name, current_process().pid)

    def notify(self, queue):
        self._queue = queue

class Stream(object):
    '''
    Representation of a record stream.
    '''

    def __init__(self, schema, sort_order, name = None):
        self._endpoints = list()
        self._schema = schema
        self._sort_order = sort_order
        self._name = name
            
    def connect(self):
        c = EndPoint()
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
            print '---------> %s' % (type(r))
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
