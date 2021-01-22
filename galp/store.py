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

    async def get_native(self, handle):
        """Requests an object, possibly interrogating the network, and waits
        until its availability is known to return.

        Note that a handle is necessary to perform a get, not just a resource
        name.
        """
        if not await self._resources.contains(handle.name):
            await self.proto.get(handle.name)
            await self._availability[handle.name].wait()
        return await self._resources.get_native(handle)

    async def put_serial(self, name, serial):
        """Put serialized object in the store, releasing all callers of
        corresponding get calls if any. 

        The underlying caching system will handle deserialization when the gets
        are resolved.
        """
        await self._resources.put_serial(name, serial)
        self._availability[name].set()

    async def not_found(self, name):
        """Signal a resource is not available, releasing all corresponding get
        callers with an error"""
        self._availability[name].set()

