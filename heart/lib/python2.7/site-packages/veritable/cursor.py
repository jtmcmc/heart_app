"""Cursor for paginated resource collections in the Veritable API.

See also: https://dev.priorknowledge.com/docs/client/python

"""

class Cursor:

    """Cursor for paginated resource collections in the Veritable API.

    Users should not initialize Cursor objects. Use these as you would
    any iterator, and only as returned by veritable methods.

    See also: https://dev.priorknowledge.com/docs/client/python

    """
    def __init__(self, connection, collection, start=None,
                 per_page=100, limit=None):        
        self.__limit = limit
        self.__start = start
        self.__per_page = per_page
        self.__connection = connection
        self.__collection = collection
        collection_key = collection.split("/")[-1]
        params = {'per_page': self.__per_page,
                  'start': self.__start}
        res = self.__connection.get(self.__collection, params=params)
        if collection_key in res:
            self.__key = collection_key
        else:
            self.__key = 'data'
        self.__next = res['links'].get('next')
        self.__last = 'next' not in res['links']
        self.__data = res.get(self.__key)

    def __str__(self):
        return "<veritable.Cursor collection='{0}' start={1} " \
            "per_page={2}>".format(self.__collection, self.__start,
            self.__per_page)

    def __repr__(self):
        return self.__str__()

    @property 
    def collection(self):
        return self.__collection

    def _refresh(self):
        if len(self.__data):
            return len(self.__data)
        if self.__next:
            res = self.__connection.get(self.__next)
        elif self.__last:
            return 0
        else:
            params = {'count': self.__per_page}
            if self.__start:
                params.update({'start': self.__start})
            res = self.__connection.get(self.__collection, params=params)
        if 'links' in res and 'next' in res['links']:
            self.__next = res['links']['next']
        else:
            self.__next = None
            self.__last = True
        self.__data = res.get(self.__key)
        return len(self.__data)

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def next(self):
        if len(self.__data) or self._refresh():
            if self.__limit is not None:
                if self.__limit == 0:
                    raise StopIteration
                self.__limit -= 1
            return self.__data.pop(0)
        else:
            raise StopIteration
