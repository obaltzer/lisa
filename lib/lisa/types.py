from shapely import wkb

class StopWord(object):
    pass
    
class IntervalInstance(tuple):
    def contains(self, other):
        return self[0] <= other < self[1]

    def intersects(self, other):
        return self[0] <= other < self[1]

    def low(self):
        return self[0]

    def high(self):
        return self[1]


class Interval(object):
    def __init__(self, base):
        if not (hasattr(base, '__lt__') or base is int or base is float):
            raise Exception, 'Base type must provide an order.'
        self._base = base

    def __call__(self, *args):
        return IntervalInstance(args)

    def __eq__(self, other):
        return other == self._base

    def intersects(self, other):
        pass

IntInterval = Interval(int)

class Geometry(object):
    def __init__(self, o):
        assert o is not None
        if type(o) is str:
            self._wkb = o
            self._geom = None
        else:
            self._geom = o
            assert self._geom is not None

    def geom(self):
        if self._geom is None:
            assert self._wkb is not None
            assert type(self._wkb) is str
            assert len(self._wkb) != 0
            self._geom = wkb.loads(self._wkb)
        return self._geom

    def __repr__(self):
        return self.geom().wkt

    def intersects(self, other):
        return self.geom().intersects(other.geom())
