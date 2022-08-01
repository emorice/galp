"""
Distributed store
"""
import asyncio
from collections import defaultdict

class Store:
    """
    Synchronization device on top of a cache system.
    """

    def __init__(self, local_storage):
        self._resources = local_storage
        self._availability = defaultdict(asyncio.Event)

    async def get_native(self, handle):
        """Requests an object, possibly interrogating the network, and waits
        until its availability is known to return.

        Note that a handle is necessary to perform a get, not just a resource
        name.
        """
        # Maybe send a request to fetch the resource first
        if not self._resources.contains(handle.name):
            await self.on_nonlocal(handle.name)

        # Maybe send requests to fetch the sub-resources if any too
        if handle.has_items:
            for part in handle:
                if not self._resources.contains(part.name):
                    await self.on_nonlocal(part.name)

        # Await for all fragments to be locally available
        await self.wait_full_resource(handle)

        # Deserialize everything and return
        return self._resources.get_native(handle)

    async def wait_resource(self, handle):
        """
        Awaits availability marker for (and only for) resources not already
        available.
        """
        if not self._resources.contains(handle.name):
            await self._availability[handle.name].wait()

    async def wait_full_resource(self, handle):
        """
        Same as wait_resource but also includes sub-resources for iterable
        hanldes.
        """
        await self.wait_resource(handle)
        if handle.has_items:
            for part in handle:
                await self.wait_resource(part)

    async def put_serial(self, name, proto, data, children):
        """Put serialized object in the store, releasing all callers of
        corresponding get calls if any.

        The underlying caching system will handle deserialization when the gets
        are resolved.
        """
        self._resources.put_serial(name, proto, data, children)
        self._availability[name].set()

    async def put_native(self, handle, native):
        """Put native object in the store, releasing all callers of
        corresponding get calls if any.

        Whether the object will be serialized in the process depends on the
        underlying cache backend.
        """
        self._resources.put_native(handle, native)
        self._availability[handle.name].set()

    async def not_found(self, name):
        """Signal a resource is not available, releasing all corresponding get
        callers with an error"""
        self._availability[name].set()

    def available(self, name):
        """Expose the internat Event for caller that just want to wait without
        actually getting the object"""
        return self._availability[name].wait()

    async def on_nonlocal(self, name):
        """
        Hook called when a resource not present in local storage was called
        """
        pass

class NetStore(Store):
    """
    Subclass of Store that includes a hook to send GET messages on missing
    resources.
    """
    def __init__(self, local_storage, proto, resolver):
        super().__init__(local_storage)
        self.proto = proto
        self.resolver = resolver

    async def on_nonlocal(self, name):
        """
        Send a `GET` over network.
        """
        route = self.resolver.get_route(name)
        await self.proto.get(route, name)
