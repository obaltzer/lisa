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

    def provides_intersect(self):
        return hasattr(self, 'intersect')

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
            anames = ', '.join([a.name() for a in self._schema])
            sql = 'SELECT %s FROM %s LIMIT 0' % (anames, self._table)
            print sql
            cursor.execute('SELECT %s FROM %s LIMIT 0' % (anames, self._table))
            #print cursor.description
            #for a in cursor.description:
            #    if a[0] not in self._schema:
            #        raise Exception('Schema not compatible.')
        except:
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
        #keys = ' AND '.join([
        #    '%s = \'%s\'' % (a.name(), v) for a, v in zip(self._schema.keys(), values)
        #])
        cursor.execute(
            'SELECT %s FROM %s WHERE %s;' % (names, self._table, keys),
            values
        )
        return cursor.fetchall()

    def schema(self):
        return self._schema

    def intersect(self, ranges):
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
        # Name of the Rtree file
        self._filename = filename
        # Name of the geometry attribute
        self._name = name

        # Construct the schema for the R-tree data source. It consists of a
        # unique OID which was generated during index creation and the
        # geometry object with the specified attribute name.
        self._schema = Schema()
        self._schema.append(Attribute('oid', int, True))
        self._schema.append(Attribute(name, Geometry))
      
        # Construct the file name of the data and data index file.
        self._data_filename = self._filename + '.data'
        self._index_filename = self._filename + '.data.idx'
        
        # Open the data and data index files
        self._data_file = open(self._data_filename, 'r+')
        self._index_file = open(self._index_filename, 'r+')

        # Determine the length of the data and the data index files.
        self._index_file.seek(0, os.SEEK_END)
        self._index_length = self._index_file.tell()
        self._index_file.seek(0)
        self._data_file.seek(0, os.SEEK_END)
        self._data_length = self._data_file.tell()
        self._data_file.seek(0)

        # Compute size of a long int.
        self._long_size = struct.calcsize('L')
        self._index_size = self._index_length / self._long_size

        # Memory-map data and data index
        self._data = mmap.mmap(self._data_file.fileno(), 0)
        self._index = mmap.mmap(self._index_file.fileno(), 0)
        
        # Open the R-tree
        self._tree = _Rtree(self._filename)

    def schema(self):
        return self._schema

    def _get_by_oid(self, oid):
        if oid < 0 or oid >= self._index_size:
            raise KeyError('Object with ID [%d] does not exist.' % (oid))
        # Compute address of object pointer.
        a = oid * self._long_size
        # Compute address of following object pointer.
        b = a + 2 * self._long_size
        # Unpack pointer to the address in the datafile.
        if b > self._index_length:
            # If the object pointer is the last one and thus there is
            # no following record, the length of the object is
            # restricted to the data file's size.
            first, = struct.unpack(
                'L', 
                self._index[a:self._index_length]
            )
            second = self._data_length
        else:
            # Otherwise simply compute the object's size from the
            # difference between its address and the address of the
            # following object.
            first, second = struct.unpack('LL', self._index[a:b])
        return (oid, Geometry(self._data[first:second]))

    def __getitem__(self, key):
        return self._get_by_oid(key['oid'])
        
    def __iter__(self):
        return self._intersect_oid((0, self._index_size))

    def _intersect_geom(self, geom):
        '''
        Returns records for which their geometry portion intersects with
        the given geometry.
        '''
        if not geom.geom().is_valid or geom.geom().area == 0.0:
            return 

        query = geom.geom().bounds
        for id, g in self._intersect_box(query):
            if g.geom().intersects(geom.geom()):
                yield (id, g)

    def _intersect_box(self, box):
        for id in self._tree.intersection(box):
            yield self._get_by_oid(id)

    def _intersect_oid(self, r):
        # Trim the range to the size of the file.
        low = r[0] >= 0 and r[0] or 0
        high = r[1] <= self._index_size and r[1] or self._index_size
        for id in range(low, high):
            yield self._get_by_oid(id)

    def intersect(self, ranges):
        if self._name in ranges and 'oid' not in ranges:
            if isinstance(ranges[self._name], Geometry):
                return self._intersect_geom(ranges[self._name])
            elif type(ranges[self._name]) is tuple:
                return self._intersect_box(ranges[self._name])
            elif ranges[self._name] is None:
                return []
            else:
                raise Exception('Invalid argument to intersect() method.')
        elif 'oid' in ranges and self._name not in ranges:
            r = ranges['oid']
            if isinstance(r, tuple):
                return self._intersect_oid(r)
            else:
                raise Exception('Invalid argument to intersect() method.')
        else:
            raise Exception('Invalid argument to intersect() method.')
            
    def __del__(self):
        self._data.close()
        self._index.close()
