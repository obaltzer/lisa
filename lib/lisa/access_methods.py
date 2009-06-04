from types import Interval

class AccessMethod(object):
    def __init__(self, data_source):
        '''
        Establishes an association between the access method and a
        particular data source resulting in a specific accessor object.
        '''
        self._data_source = data_source
    
    def accepts(self, schema):
        '''
        Returns True when the access method can accept queries with the
        specified schema, otherwise it returns False.
        '''
        return False

    def query(self, q):
        '''
        Evaluates the given query on the data contained in the data sotrage
        and returns a generator over the result records.
        '''
        yield None

class FindIdentities(AccessMethod):
    def __init__(self, data_source):
        AccessMethod.__init__(self, data_source)
       
        if data_source.provides_random_access():
            self.query = self._query_random_access
        else:
            self.query = self._query_iterator
        
    def accepts(self, schema):
        # print 'Other: %s' % (schema)
        # print 'Self: %s' % (self._data_source.schema())
        for a in schema:
            if a not in self._data_source.schema():
                return False
        return True

    def _query_iterator(self, schema, q):
        # print 'query_iterator: %s' % (q)
        indices = []
        i = 0
        for a in schema:
            indices.append((i, self._data_source.schema().index(a)))
            i += 1
        for r in self._data_source:
            match = True
            for a, b in indices:
                match &= r[b] == q[a]
            if match:
                yield r

    def _query_random_access(self, schema, q):
        # print 'query_random_access: %s' % (q)
        query = {}
        for i, a in enumerate(schema):
            if a in self._data_source.schema().keys():
                query[a.name()] = q[i]
        return self._data_source[query]

class FindRange(AccessMethod):
    def __init__(self, data_source):
        AccessMethod.__init__(self, data_source)
        if data_source.provides_intersect():
            self.query = self._query_intersect
        # Cannot use random access since the given range may not be
        # discretizable
        #elif data_source.provides_random_access():
        #    self.query = self._query_random_access
        elif data_source.provides_iterator():
            self.query = self._query_iterator
        else:
            raise Exception('Access Method not supported by data source.')
    
    def accepts(self, schema):
        found = False
        for a in schema:
            if not hasattr(a.type(), 'intersects'):
                return False
            else:
                for b in self._data_source.schema():
                    if a == b:
                       found |= True 
        return found

    def _query_iterator(self, schema, q):
        indices = []
        for i, a in enumerate(schema):
            indices.append((i, self._data_source.schema().index(a)))
        for r in self._data_source:
            match = True
            for a, b in indices:
                match &= q[a].intersects(r[b])
            if match:
                yield r

    def _query_intersect(self, schema, q):
        ranges = {}
        indices = []
        for i, a in enumerate(schema):
            ranges[a.name()] = q[i]
            indices.append((i, self._data_source.schema().index(a)))
        
        retrieved = 0
        returned = 0
        for r in self._data_source.intersect(ranges):
            retrieved += 1
            match = True
            for a, b in indices:
                match &= q[a].intersects(r[b])
            if match:
                returned += 1
                yield r
        #print 'Retrieved: ', retrieved
        #print 'Returned: ', returned
