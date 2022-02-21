"""
Resolver.
"""

class UnresolvableResourceError(Exception):
    def __init__(self, name):
        self.printable_name = name.hex()

class Resolver:
    def __init__(self):
        """
        Maps resource names to a route that can be queried from it.

        The simplest implementation requires routes to be pre-recorded.
        """
        self._routes = {}

    def get_route(self, name):
        try:
            return self._routes[name]
        except KeyError:
            raise UnresolvableResourceError(name)

    def set_route(self, name, route):
        self._routes[name] = route
