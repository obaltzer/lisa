class Attribute(object):
    def __init__(self, name, type, key = False):
        self._name = name
        self._type = type
        self._key = key

    def __eq__(self, other):
        if type(other) is self.__class__:
            return other._name == self._name and other._type == self._type
        elif type(other) is str:
            return other == self._name
        else:
            return False

    def __repr__(self):
        return 'Attribute(name = %s, type = %s)' % (self._name, self._type)

    def type(self):
        return self._type

    def name(self):
        return self._name

    def key(self):
        return self._key

class Schema(list):
    def __init__(self, *args, **kwargs):
        list.__init__(self, *args, **kwargs)
        self._map = dict()
        self._name_map = dict()

    def append(self, attribute):
        self._map[attribute] = len(self)
        self._name_map[attribute.name()] = len(self)
        list.append(self, attribute)

    def __contains__(self, other):
        '''
        Tests if other is included in the schema. If other is a sequence
        the method only returns True if all elements of the sequence are
        included in the schema.
        '''
        if isinstance(other, list) or isinstance(other, tuple):
            ret = True
            for i in other:
                if i not in self._map:
                    return False
            return True
        elif isinstance(other, Attribute):
            return other in self._map
        elif type(other) is str:
            return other in self._name_map
        else:
            return other in self

    def index(self, attribute):
        if isinstance(attribute, Attribute):
            try:
                return self._map[attribute]
            except:
                return list.index(self, attribute)
        elif type(attribute) is str:
            try:
                return self._name_map[attribute]
            except:
                return list.index(self, attribute)
        else:
            list.index(self, attribute)

    def keys(self):
        return [a for a in self if a.key()]
