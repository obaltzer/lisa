from shapely import wkb

class StopWord(object):
    pass

class Interval(object):
    class Instance(tuple):
        def contains(self, other):
            return self[0] <= other < self[1]

        def low(self):
            return self[0]

        def high(self):
            return self[1]

    def __init__(self, base):
        if not hasattr(base, '__lt__'):
            raise Exception, 'Base type must provide an order.'
        self._base = base

    def __call__(self, *args):
        return self.Instance(args)

    def __eq__(self, other):
        return other == self._base

    def contains(self, other):
        pass

IntInterval = Interval(int)

class Geometry(object):
    def __init__(self, wkb):
        self._wkb = wkb
        self._geom = None

    def geom(self):
        if not self._geom:
            self._geom = wkb.loads(self._wkb)
        return self._geom

    def __repr__(self):
        return self.geom().wkt

    def contains(self, other):
        return self.geom().contains(other.geom())
