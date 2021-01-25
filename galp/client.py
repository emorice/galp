"""
Client api.

In contrast with a worker which is written as a standalone process, a client is just an
object that can be created and used as part of a larger program.
"""

import logging
import json
import asyncio

import zmq
import zmq.asyncio

from collections import defaultdict

from galp.cache import CacheStack
from galp.serializer import Serializer
from galp.protocol import Protocol
from galp.store import Store

class Client(Protocol):
    """
    A client that communicate with a worker.

    Either socket or endpoint must be specified, never both. Socket is intended
    for testing, and custom socket manipulation. Endpoint also uses pyzmq's global
    context instance, creating it if needed -- and implicitely reusing it.

    Args:
        socket: a ZeroMQ asyncio socket, already connected to the worker endpoint.
            The client will not attempt to close it. No read operation must be
            attempted by external code while the client exists. New write,
            connect and binds should not be a problem.
       endpoint: a ZeroMQ endpoint string to the worker. The client will create
            its own socket and destroy it in the end, using the global sync context.
    """

    # For our sanity, below this point, by 'task' here we need task _name_, except for the
    # 'tasks' public argument itself. The tasks themselves are called 'details'

    def __init__(self, socket=None, endpoint=None):
        if (socket is None ) == (endpoint is None):
            raise ValueError('Exactly one of endpoint or socket must be specified')

        self.close_socket = False
        if socket is None:
            socket = zmq.asyncio.Context.instance().socket(zmq.DEALER)
            socket.connect(endpoint)
            self.close_socket = True
        self.socket = socket

        # Public attributes: counters for the number of SUBMITs sent and DOING
        # received for each task
        # Used for reporting and testing cache behavior
        self.submitted_count = defaultdict(int)
        self.run_count = defaultdict(int)

        self._details = dict()
        self._dependents = defaultdict(set)
        self._dependencies = defaultdict(set)
        self._done = defaultdict(bool) # def to False

        # Tasks whose result is to be fetched
        self._finals = set()

        # Memory only native+serial cache
        self._cache = CacheStack(
            dirpath=None,
            serializer=Serializer())
        self._store = Store(self._cache)

        # Start only one processing loop
        self._processor = None
        self._collections = 0

    def __delete__(self):
        if self.close_socket:
            self.socket.close()

    def add(self, tasks):
        """
        Browse the graph and add it to the tasks we're tracking, in an
        idempotent way. Also tracks which ones are final.

        The client can keep references to any task passed to it directly or as
        a dependency. Modifying tasks after there were added, directly or as
        dependencies to any degree, results in undefined behaviour. Creating new
        tasks depending on already added ones, however, is safe. In other words,
        you can add new downstream steps, but not change the upstream part.

        Return the names of all new tasks without dependencies. 

        This justs updates the client's state, so it can never block and is a
        sync function.
        """

        for task_details in tasks:
            self._finals.add(task_details.name)

        # Build the inverse dependencies lookup table, i.e the 'children' task.
        # child/parent can be ambiguous here (what is the direction of the
        # arcs ?) so we call them "dependents" as opposed to "dependencies"

        # Todo: Optimisation: sets and defaultdicts make most operations safely
        # idempotent, but we could skip tasks added in a previous call
        oset, cset = set(tasks), set()

        new_top_level = set()
        while oset:
            # Get a task
            task_details = oset.pop()
            task = task_details.name
            cset.add(task)

            # Add the links
            for dep_details in task_details.dependencies:
                dep = dep_details.name
                self._dependencies[task].add(dep)
                self._dependents[dep].add(task)
                if dep not in cset:
                    oset.add(dep_details)

            # Check unseen input tasks
            if not self._dependencies[task] and task not in self._details:
                new_top_level.add(task)

            # Save details, and mark as seen
            self._details[task] = task_details

        return new_top_level

    async def process(self):
        """
        Reacts to messages.
        """
        # Todo: timeouts
        logging.warning('Now reacting to completion events')
        terminate = False
        try:
            while not terminate:
                msg = await self.socket.recv_multipart()
                terminate = await self.on_message(msg)
        except asyncio.CancelledError:
            pass
        logging.warning('Message processing stopping')

    async def collect(self, *tasks):
        """
        Recursively submit the tasks, wait for completion, fetches, deserialize
        and returns the actual results.

        Written as a coroutine, so that it can be called inside an asynchronous
        application, such as a worker or proxy or whatever: the caller is in
        charge of the event loop.
        """

        # Update our state
        new_inputs = self.add(tasks)

        for task in new_inputs:
            await self.submit(task)

        if self._processor is None:
            self._processor = asyncio.create_task(self.process())
        # Not thread-safe but ok in coop mt
        self._collections += 1

        # Note that we need to go the handle since this is where we deserialize.
        results =  await asyncio.gather(*(
            self._store.get_native(task.handle) for task in tasks
            ))

        self._collections -= 1
        if not self._collections:
            proc = self._processor
            # Note: here the processor could still be running and a new collect
            # start, thus starting a new processor, hence the processor has to
            # be reentrant
            self._processor = None
            proc.cancel()
            await proc

        return results

    # Custom protocol sender
    # ======================

    async def submit(self, task_name):
        """Loads details, handles hereis, and manage stats"""
        details = self._details[task_name]

        # Hereis could be put in cache or store, store handles the weird edge
        # case where a hereis is collected
        if hasattr(details, 'hereis'):
            await self._store.put_native(details.handle, details.hereis)
            await self.on_done(task_name)
            return

        r = await super().submit(details)
        self.submitted_count[task_name] += 1
        return r

    # Protocol callbacks
    # ==================
    async def send_message(self, msg):
        await self.socket.send_multipart(msg)

    async def on_get(self, name):
        # Note: we purposely do not use store here, since we could be receiving
        # GETs for resources we do not have and store blocks in these cases.
        try:
            await self.put(name, await self._cache.get_serial(name))
            logging.warning('Client GET on %s', name.hex())
        except KeyError:
            await self.not_found(name)
            logging.warning('Client missed GET on %s', name.hex())

    async def on_put(self, name, obj):
        """
        Cannot actually block but async anyway for consistency.

        Not that store will release the corresponding collects if needed.
        """
        await self._store.put_serial(name, obj)

    async def on_done(self, done_task):
        """Given that done_task just finished, mark it as done, submit any
        dependent ready to be submitted, and send GETs for final tasks.

        """

        self._done[done_task] = True

        for dept in self._dependents[done_task]:
            if all(self._done[sister_dep] for sister_dep in self._dependencies[dept]):
                await self.submit(dept)

        # FIXME: we should skip that for here-is tasks, but that's a convoluted,
        # unsupported case for now
        if done_task in self._finals:
            await self.get(done_task)

    async def on_doing(self, task):
        """Just updates statistics"""
        logging.warning('Doing: %s', task.hex())
        self.run_count[task] += 1

    async def on_illegal(self):
        """Should never happen"""
        assert False
