import sys
import random
import os.path

if len(sys.argv) != 2:
    print 'Please provide dataset specification.'
    sys.exit(-1)

filename = sys.argv[1]

if not os.path.isfile(filename):
    print 'Specified file [%s] does not exist.' % (filename)
    sys.exit(-1)

f = open(filename, 'r')
config = compile(''.join(f.readlines()), filename, 'exec')
eval(config)
f.close()

counters = {}

def get_id(level):
    if level in counters:
        counters[level] += 1
    else:
        counters[level] = 0
    return counters[level]
    
def generate_level(level, hierarchy):
    slack = hierarchy[level]['slack']
    fanout = hierarchy[level]['cardinality']
    name = hierarchy[level]['name']
    f = random.randint(
        int(fanout * (1.0 - slack)), 
        int(fanout * (1.0 + slack))
    )
    if level < len(hierarchy) - 1:
        for i in xrange(f):
            yield (
                get_id(name), 
                generate_level(level + 1, hierarchy)
            )
    else:
        for i in xrange(f):
            yield (get_id(name), None)

def print_level(l, i = 0):
    for x in l:
        if x[1]:
            print '\n' + (' ' * i) + '%d:' % (x[0]),
            print_level(x[1], i + 2)
        else:
            print ', '.join('%d' % x[0] for x in l),
            return

def sql_tables(hierarchy):
    for i, l in enumerate(hierarchy):
        name = l['name']
        if i == 0:
            print 'CREATE TABLE %s (id INTEGER PRIMARY KEY);' % (name)
            print 'CREATE INDEX %s_id ON %s (id);' % (name, name)
        else:
            prev = hierarchy[i - 1]['name']
            print ('CREATE TABLE %s (' + \
                        'id INTEGER PRIMARY KEY, ' + \
                        '%s_id INTEGER REFERENCES %s (id)' + \
                    ');') % \
                (
                    name,
                    prev,
                    prev,
                )
            print 'CREATE INDEX %s_id ON %s (id);' % (name, name)

def sql_level(hierarchy, values, i = 0, parent = None):
    n = hierarchy[i]['name']
    for x in values:
        if i:
            print 'INSERT INTO %s VALUES (%d, %d);' % (n, x[0], parent)
        else:
            print 'INSERT INTO %s VALUES (%d);' % (n, x[0])
        if x[1]:
            sql_level(hierarchy, x[1], i + 1, x[0])

def sql_measures(measures, hierarchy):
    for m in measures:
        n = m['name']
        l = m['association']
        a = ' '.join('%s INTEGER,' % (x[0]) for x in m['attributes'])
        print ('CREATE TABLE %s (' + \
                    'id INTEGER PRIMARY KEY,' + \
                    '%s' + \
                    '%s_id INTEGER REFERENCES %s (id)' + \
                ');') % (n, a, l, l)
        for i in xrange(int(m['count'])):
            a = ' '.join(
                '%d,' % (random.randint(v[0], v[1])) for k,v in m['attributes']
            )
            print 'INSERT INTO %s VALUES (%d,%s %d);' % (
                n,
                i,
                a,
                random.randint(0, counters[l])
            )

# print_level(generate_level(0, fanout))
sql_tables(hierarchy)
print 'BEGIN;'
sql_level(hierarchy, generate_level(0, hierarchy))
print 'COMMIT;'

print 'BEGIN;'
sql_measures(measures, hierarchy)
print 'COMMIT;'
