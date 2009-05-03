from shapely import wkb

class StopWord(object):
    pass

class Interval(object):
    class Instance(object):
        def __init__(self, low, high):
            self._l = low
            self._h = high

        def __gt__(self, other):
            if isinstance(other, self.__class__):
                return self._l <= other._l and other._h <= self._h
            else:
                return self._l <= other < self._h
        
        def __lt__(self, other):
            if isinstance(other, self.__class__):
                return other._l <= self._l and self._h < self._h
            else:
                return other < self._l or self._h <= other

        def __repr__(self):
            return '(%s, %s)' % (self._l, self._h)

        def low(self):
            return self._l

        def high(self):
            return self._h

    def __init__(self, base):
        if not hasattr(base, '__lt__'):
            raise Exception, 'Base type must provide an order.'
        self._base = base

    def __call__(self, *args):
        return self.Instance(*args)

    def __eq__(self, other):
        return other == self._base

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
