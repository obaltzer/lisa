from multiprocessing.queues import JoinableQueue as Queue
from Queue import Empty
from multiprocessing import Lock, current_process

from schema import Schema
import logging

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
    def __init__(self, size = 1):
        Queue.__init__(self, size)
        self._closed = False
        self._queue = None
        self._name = end_point_counter()
        self.log = logging.getLogger('EndPoint[%04d]' % (self._name))
        self.log.info('EndPoint initialized.')

    def name(self):
        return self._name
        
    def receive(self, block = True):
        if self._closed:
            raise StreamClosedException
        try:
            self.log.debug('[%5d(%-20s)]: get enter' % (
                current_process().pid, 
                str(current_process().name),
            ))
            o = self.get(block)
            self.task_done()
            self.log.debug('[%5d(%-20s)]: get exit' % (
                current_process().pid, 
                str(current_process().name),
            ))
        
            if type(o) is StreamEnd:
                self.log.debug('[%5d(%-20s)]: closing stream' % (
                    current_process().pid,
                    str(current_process().name),
                ))
                self._closed = True
                raise StreamClosedException
            else:
                self.log.debug('[%5d(%-20s)]: return' % (
                    current_process().pid,
                    str(current_process().name),
                ))
                return o
        except Empty:
            self.log.debug('[%5d(%-20s)]: get exit (EMPTY)' % (
                current_process().pid, 
                str(current_process().name),
            ))
            raise

    def processed(self):
        # self.task_done()
        pass

    def close(self):
        self.send(StreamEnd(), True)

    def closed(self):
        return self._closed

    def send(self, o, flush = False):
        self.log.debug('[%5d(%-20s)]: put enter' % (
            current_process().pid,
            str(current_process().name)
        ))
        self.put(o)
        self.log.debug('[%5d(%-20s)]: put exit' % (
            current_process().pid,
            str(current_process().name)
        ))
        if self._queue:
            self.log.debug('[%5d(%-20s)]: notify put enter' % (
                current_process().pid,
                str(current_process().name)
            ))
            self._queue.put(self._name)
            self.log.debug('[%5d(%-20s)]: notify put exit' % (
                current_process().pid,
                str(current_process().name)
            ))

    def notify(self, queue):
        self.log.debug('[%5d(%-20s)]: setting notification queue' % (
            current_process().pid,
            str(current_process().name),
        ))
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

class DemuxStream(object):
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
                    print 'Updating for end point: [%d]' % (self._i)
                    self._channel._update(self._i)
            except StreamClosedException:
                if self._queue.empty():
                    raise
                else:
                    pass
            print 'Returning for end point: [%d]' % (self._i)
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
            print '\t%s: update called' % (ep)
            self._lock.acquire()
            print '\t%s: lock acquired' % (ep) 
            print '[%s] Channel status (closed = %s)' % (current_process().pid, self._closed)
            if self._closed:
                print '\t%s: stream closed' % (ep)
                self._lock.release()
                print '\t%s: lock released' % (ep)
                raise StreamClosedException
            try:
                print '\t%s: trying receive' % (ep)
                r = self._demux._receive()
                print '\t%s: record received' % (ep)
                for e in self._ep:
                    print '\t%s: sending to ep: %s' % (ep, e._i)
                    e.send(r)
            except StreamClosedException:
                self._closed = True
                print '\t%s: closing' % (ep)
                raise
            finally:
                print '[%s] Channel status (closed = %s)' % (current_process().pid, self._closed)
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

#    def __repr__(self):
#        return '<Demux: %s >' % (self._stream)

    def channel(self):
        c = self.Channel(self)
        self._channels.append(c)
        return c

    def _receive(self, block = True):
        print 'Demux: receive called'
        self._lock.acquire()
        print 'Demux: lock acquired'
        print 'Demux[%s]: status (closed = %s)' % (str(self), self._closed)
        print 'Demux[%s]: stream closed %s' % (str(self), self._closed)
        if self._closed:
            print 'Demux: stream closed...throw exception'
            self._lock.release()
            raise StreamClosedException
        try:
            print 'Demux: calling endpoint receive'
            v = self._ep.receive(block)
            print 'Demux: receive complete'
            return v
        except StreamClosedException:
            self._closed = True
            print 'Demux[%s]: stream was closed: %s' % (str(self), self._closed)
            raise
        finally:
            print 'Demux: releasing lock'
            print 'Demux[%s]: status (closed = %s)' % (str(self), self._closed)
            self._lock.release()
