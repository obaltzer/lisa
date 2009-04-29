from Queue import Queue
from threading import Lock

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
            
        def receive(self, block = True):
            o = self.get(block)
            if type(o) is Stream.StreamEnd:
                self.task_done()
                raise StreamClosedException
            else:
                return o

        def processed(self):
            self.task_done()

        def close(self):
            # print 'sending StreamEnd'
            self.put(Stream.StreamEnd())
            if self._queue:
                # print 'put close into queue'
                self._queue.put(self)
                # print 'put close into queue...done'
 
        def send(self, o):
            self.put(o)
            if self._queue:
                self._queue.put(self)

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
