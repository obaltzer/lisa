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
    def accepts(self, schema):
        for a in schema:
            if a not in self._data_source.schema():
                return False
        return True

    def query(self, schema, q):
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

class FindRange(AccessMethod):
    def accepts(self, schema):
        found = False
        for a in schema:
            if not isinstance(a.type(), Interval):
                return False
            else:
                for b in self._data_source.schema():
                    if a == b:
                       found |= True 
        return found

    def query(self, schema, q):
        indices = []
        i = 0
        for a in schema:
            indices.append((i, self._data_source.schema().index(a)))
            i += 1
        for r in self._data_source:
            match = True
            for a, b in indices:
                match &= q[a].low() <= r[b] < q[a].high()
            if match:
                yield r
