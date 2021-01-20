"""
Distributed store
"""
import asyncio
from collections import defaultdict

class Store:
    """
    Helper class for the distributed object store role.

    Keeps a store of resources, handles get and put requests, and manages events
    to provide synchronous interfaces when needed.
    """

    def __init__(self, local_storage, proto):
        self._resources = local_storage
        self.proto = proto
        self._availability = defaultdict(asyncio.Event)
        self._handles = dict()

    async def get(self, handle):
        """Requests an object, possibly interrogating the network, and waits
        until its availability is known to return.

        Note that a handle is necessary to perform a get, not just a resource
        name.
        """
        if not await self._resources.contains(handle.name):
            self._handles[handle.name] = handle
            await self.proto.get(handle.name)
            await self._availability[handle.name].wait()
        return await self._resources.get(handle)

    async def put(self, name, obj):
        """Put native object in the store, releasing all callers of
        corresponding get calls.

        This operation will only succeed if a previous `get` call for the
        corresponding resource has been made, and metadata for the resource is
        available.

        To preload an object in the store, directly load it in the underlying
        storage."""

        try:
            await self._resources.put(self._handles[name], obj)
            self._availability[name].set()
        except KeyError:
            raise ValueError('Trying to put in store unrequested resource')

    async def not_found(self, name):
        """Signal a resource is not available, releasing all corresponding get
        callers with an error"""
        self._availability[name].set()

