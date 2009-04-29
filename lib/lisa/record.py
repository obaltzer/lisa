from schema import Schema

class Record(tuple):
    def __init__(self, schema, *args):
        self._schema = schema
        assert len(schema) == len(args)
        tuple.__init__(args)
