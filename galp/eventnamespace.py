"""
Utils to handle collections of events
"""

class EventNamespace:
    def __init__(self):
        self._handlers = {}

    def on(self, event):
        """Decorator to register event."""
        def _register(handler, event=event):
            if event in self._handlers:
                raise AlreadyRegisteredError
            self._handlers[event] = handler
            return handler
        return _register

    def handler(self, event):
        """Return handler.

        Note that we cannot call it directly, as we do not know its color, you
        could have synchronous or asynchronous handlers. Also handler may not
        even necessarily be a callable.
        """
        try:
            return self._handlers[event]
        except KeyError:
            raise NoHandlerError
            
class NoHandlerError(Exception):
    """No handler for given exception"""
    pass

class AlreadyRegisteredError(Exception):
    """Previous handler defined for given exception"""
    pass
