class VeritableError(Exception):
    """Base class for exceptions in veritable-python."""
    def __init__(self, value, **kwargs):
        self.value = value
        for k, v in kwargs.items():
            self.__dict__[k] = v

    def __str__(self):
        return repr(self.value)
