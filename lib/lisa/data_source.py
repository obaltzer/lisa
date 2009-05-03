import sqlite3
from threading import current_thread

from types import Geometry
from schema import Schema, Attribute

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

class DBTable(DataSource):
    def __init__(self, database, table, schema):
        self._schema = schema
        self._table = table
        self._database = database
        self._thread_conn = {}
        self._conn = None

    def _verify_schema(self):
        cursor = self._conn.cursor()
        try:
            cursor.execute('SELECT * FROM %s LIMIT 0' % (self._table))
            for a in cursor.description:
                if a[0] not in self._schema:
                    raise Exception('Schema not compatible.')
        finally:
            cursor.close()

    def _setup(self):
        if current_thread() not in self._thread_conn:
            self._thread_conn[current_thread()] = \
                sqlite3.connect(self._database)
            self._conn = self._thread_conn[current_thread()]
            self._verify_schema()
        else:
            self._conn = self._thread_conn[current_thread()]
        
    def __iter__(self):
        self._setup()
        
        cursor = self._conn.cursor()
        names = ', '.join([a.name() for a in self._schema])
        cursor.execute('SELECT %s FROM %s' % (names, self._table))
        # determine mapping between attributes
        for values in cursor:
            r = tuple(a.type()(v) for (a, v) in zip(self._schema, values))
            yield r
        cursor.close()

    def __getitem__(self, key):
        self._setup()
        
        cursor = self._conn.cursor()
        names = ', '.join([a.name() for a in self._schema])
        keys = ' AND '.join([
            '%s = ?' % (a.name()) for a in self._schema.keys()
        ])
        values = tuple(key[a.name()] for a in self._schema.keys())
        cursor.execute(
            'SELECT %s FROM %s WHERE %s' % (names, self._table, keys),
            values
        )
        return cursor.fetchone()

    def schema(self):
        return self._schema

    def contained(self, ranges):
        self._setup()

        cursor = self._conn.cursor()
        names = ', '.join([a.name() for a in self._schema])
        cond = ' AND '.join(
            ['(%s >= ? AND %s < ?)' % (k, k) for k in ranges]
        )
        values = []
        for k in ranges:
            values.append(ranges[k][0])
            values.append(ranges[k][1])
        cursor.execute(
            'SELECT %s FROM %s WHERE %s' % (names, self._table, cond),
            tuple(values)
        )
        for row in cursor:
            r = tuple(a.type()(v) for (a, v) in zip(self._schema, row))
            yield r
        cursor.close()

from shapely import wkb
from rtree import Rtree as _Rtree

import sys
import os
import mmap
import struct

class Rtree(DataSource):
    def __init__(self, filename, name):
        self._filename = filename
        self._name = name
        self._schema = Schema()
        self._schema.append(Attribute('oid', int, True))
        self._schema.append(Attribute(name, Geometry))
        
        self._data_filename = self._filename + '.data'
        self._index_filename = self._filename + '.data.idx'
        self._tree = _Rtree(self._filename)

        self._data_file = open(self._data_filename, 'r+')
        self._index_file = open(self._index_filename, 'r+')

        self._index_file.seek(0, os.SEEK_END)
        self._index_length = self._index_file.tell()
        self._index_file.seek(0)

        self._data_file.seek(0, os.SEEK_END)
        self._data_length = self._data_file.tell()
        self._data_file.seek(0)

        self._long_size = struct.calcsize('L')

        self._data = mmap.mmap(self._data_file.fileno(), 0)
        self._index = mmap.mmap(self._index_file.fileno(), 0)

    def schema(self):
        return self._schema

    def __getitem__(self, key):
        # Should just return the key-th object from index and data file.
        pass

    def __iter__(self):
        # Iterate over all keys in the index and return objects from data
        # file.
        pass

    def contained(self, ranges):
        # Handle ranges over OIDs separate from ranges over geometries.
        query = ranges[self._name].geom().bounds
        for id in self._tree.intersection(query):
            a = id * self._long_size
            b = a + 2 * self._long_size
            if b > self._index_length:
                first, = struct.unpack(
                    'L', 
                    self._index[a:self._index_length]
                )
                second = self._data_length
            else:
                first, second = struct.unpack('LL', self._index[a:b])
            yield (id, Geometry(self._data[first:second]))

    def __del__(self):
        self._data.close()
        self._index.close()
