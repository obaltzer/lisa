from schema import Schema

class DataSource(object):
    def schema(self):
        '''
        Returns the schema of the data stored in the data source.
        '''
        return Schema()
 
    def provides_random_access(self):
        return hasattr(self, '__getitem__')

    def provides_iterator(self):
        return hasattr(self, '__iter__')

class CSVFile(DataSource):
    def __init__(self, filename, schema):
        self._schema = schema
        self._filename = filename

    def schema(self):
        return self._schema

    def __iter__(self):
        f = open(self._filename, 'r')
        # read the header and determine column order
        names = f.readline().strip().split(',')
        indices = []
        for a in self._schema:
            indices.append((a, names.index(a.name())))

        # For each remaining line in the file, extract the values and put
        # them in the correct order for the record.
        for l in f:
            values = l.strip().split(',')
            r = tuple(a.type()(values[i]) for (a, i) in indices)
            yield r
        f.close()
