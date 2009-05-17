from lisa.schema import Schema, Attribute

class UniversalSelect(object):
    def __init__(self, input_schema, mapping):
        '''
        mapping = {
            'name': {
                'type': type, 
                'args': ['input', 'input', ...], 
                'function': function
            },
            ...
        }
        '''
        self._input_schema = input_schema
        self._schema = Schema()
        self._mapping = mapping
        self._f = []
        for name in mapping:
            # Create output schema type
            self._schema.append(Attribute(
                name,
                mapping[name]['type'],
            ))
            # Verify input schema and mapping
            for n in mapping[name]['args']:
                if n not in self._input_schema:
                    raise Exception('Incompatible schema.')

            self._f.append((
                [input_schema.index(n) for n in mapping[name]['args']],
                mapping[name]['function'],
            ))

    def schema(self):
        return self._schema

    def accepts(self, other):
        return self._input_schema == other

    def __call__(self, r):
        return tuple(
            f[1](*[r[i] for i in f[0]]) for f in self._f
        )
