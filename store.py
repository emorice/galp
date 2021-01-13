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

    async def get(self, name):
        """Requests an object, possibly interrogating the network, and waits
        until its availability is known to return.
        """
        if not await self._resources.contains(name):
            await self.proto.get(name)
            await self._availability[name].wait()
        return await self._resources.get(name)

    async def put(self, name, obj):
        """Put native object in the store, releasing all callers of
        corresponding get calls"""
        await self._resources.put(name, obj)
        self._availability[name].set()

    async def not_found(self, name):
        """Signal a resource is not available, releasing all corresponding get
        callers with an error"""
        self._availability[name].set()

