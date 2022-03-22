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

from enum import IntEnum, auto

from collections import defaultdict

from galp.cache import CacheStack
from galp.serializer import Serializer
from galp.protocol import Protocol
from galp.store import Store
from galp.graph import Handle


class TaskStatus(IntEnum):
    UNKNOWN = auto()
    FAILED = auto()
    DEPEND = auto()

    SUBMITTED = auto()
    RUNNING = auto()

    # Note: everything that compares greater or equal to completed implies
    # completed
    COMPLETED = auto()
    TRANSFER = auto()
    AVAILABLE = auto()

class TaskFailedError(RuntimeError):
    pass

class Client(Protocol):
    """
    A client that communicate with a worker.

    The endpoint must be specified. The client uses pyzmq's global
    context instance, creating it if needed -- and implicitely reusing it.

    Args:
       endpoint: a ZeroMQ endpoint string to the worker. The client will create
            its own socket and destroy it in the end, using the global sync context.
    """

    # For our sanity, below this point, by 'task' here we need task _name_, except for the
    # 'tasks' public argument itself. The tasks themselves are called 'details'

    def __init__(self, endpoint):

        socket = zmq.asyncio.Context.instance().socket(zmq.DEALER)
        socket.connect(endpoint)
        self.socket = socket

        # With a DEALER socket, we send all messages with no routing information
        self.route = []

        # Public attributes: counters for the number of SUBMITs sent and DOING
        # received for each task
        # Used for reporting and testing cache behavior
        self.submitted_count = defaultdict(int)
        self.run_count = defaultdict(int)

        self._details = dict()
        self._dependents = defaultdict(set)
        self._dependencies = defaultdict(set)

        #self._done = defaultdict(bool) # def to False
        ## Richer alternative
        self._status = defaultdict(lambda: TaskStatus.UNKNOWN)

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

        Return the names of all new tasks without uncompleted dependencies. 

        This justs updates the client's state, so it can never block and is a
        sync function.
        """

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
            if (
                # Note: True if no dependencies at all
                all(
                    self._status[dep] >= TaskStatus.COMPLETED
                    for dep in self._dependencies[task]
                    )
                and task not in self._details
                ):
                new_top_level.add(task)

            # Save details, and mark as seen
            self._details[task] = task_details

        return new_top_level

    async def add_finals(self, tasks):
        """
        Add tasks to the set of finals (= to be collected) tasks, and send a GET
        for any new one that was already done.
        """

        for task_details in tasks:
            if (
                task_details not in self._finals
                and
                self._status[task_details.name] is TaskStatus.COMPLETED
                ):
                logging.warning("Sending extra GET for upgraded %s",
                    task_details.name.hex())
                await self.get(task_details.name)
            self._finals.add(task_details.name)

    async def process(self):
        """
        Reacts to messages.
        """
        # Todo: timeouts
        logging.info('Now reacting to completion events')
        terminate = False
        try:
            while not terminate:
                msg = await self.socket.recv_multipart()
                #terminate = await self.on_message(msg)
                asyncio.create_task(self.on_message(msg))
        except asyncio.CancelledError:
            pass
        logging.info('Message processing stopping')

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

        # Send SUBMITs for all possibly new and ready downstream tasks
        for task in new_inputs:
            await self.submit(task)

        # Update the list of final tasks and send GETs for the already done 
        # Note: comes after submit as derived task need to be submitted first to
        # be marked as completed before.
        await self.add_finals(tasks)


        if self._processor is None:
            self._processor = asyncio.create_task(self.process())
        # Not thread-safe but ok in coop mt
        self._collections += 1

        try:
            # Note that we need to go the handle since this is where we deserialize.
            results =  await asyncio.gather(*(
                self._store.get_native(task.handle) for task in tasks
                ))
        except KeyError:
            # The store returned without the object available, meaning something
            # failed
            raise TaskFailedError

        finally:
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
    # For simplicity these set the route inside them

    async def submit(self, task_name):
        """Loads details, handles hereis and subtasks, and manage stats"""
        details = self._details[task_name]

        # Reentrance check
        if self._status[task_name] >= TaskStatus.SUBMITTED:
            return False
        self._status[task_name] = TaskStatus.SUBMITTED

        # Hereis could be put in cache or store, store handles the weird edge
        # case where a hereis is collected
        if hasattr(details, 'hereis'):
            await self._store.put_native(details.handle, details.hereis)
            await self.on_done(task_name)
            return
        # Sub-tasks are automatically satisfied when their parent and unique
        # dependency is
        if hasattr(details, 'parent'):
            await self.on_done(task_name)
            return

        r = await super().submit(self.route, details)
        self.submitted_count[task_name] += 1
        return r

    async def get_once(self, task_name):
        # Reentrance check
        if self._status[task_name] >= TaskStatus.TRANSFER:
            return False
        self._status[task_name] = TaskStatus.TRANSFER
        return await super().get(self.route, task_name)

    def get(self, task_name):
        return self.get_once(task_name)

    # Protocol callbacks
    # ==================
    async def send_message(self, msg):
        await self.socket.send_multipart(msg)

    async def on_get(self, route, name):
        # Note: we purposely do not use store here, since we could be receiving
        # GETs for resources we do not have and store blocks in these cases.
        try:
            await self.put(route, name, *self._cache.get_serial(name))
            logging.debug('Client GET on %s', name.hex())
        except KeyError:
            await self.not_found(route, name)
            logging.warning('Client missed GET on %s', name.hex())

    async def on_put(self, route, name, proto: bytes, data: bytes, children: int):
        """
        Cannot actually block but async anyway for consistency.

        Not that store will release the corresponding collects if needed.
        """

        handle = (
            self._details[name].handle
            if name in self._details
            else Handle(name)
            )

        # Send sub-gets if necessary
        for i in range(children):
            await self.get_once(handle[i].name)

        # Wait for all subs to be resolved
        for i in range(children):
            await self._store.available(handle[i].name)

        # Put the last part, thus releasing waiters.
        await self._store.put_serial(name, proto, data, children.to_bytes(1, 'big'))
        self._status[name] = TaskStatus.AVAILABLE

    async def on_done(self, done_task):
        """Given that done_task just finished, mark it as done, submit any
        dependent ready to be submitted, and send GETs for final tasks.

        """

        self._status[done_task] = TaskStatus.COMPLETED

        # FIXME: we should skip that for here-is tasks, but that's a convoluted,
        # unsupported case for now
        if done_task in self._finals:
            await self.get(done_task)

        for dept in self._dependents[done_task]:
            if all(self._status[sister_dep] >= TaskStatus.COMPLETED for sister_dep in self._dependencies[dept]):
                await self.submit(dept)


    async def on_doing(self, task):
        """Just updates statistics"""
        self.run_count[task] += 1
        self._status[task] = TaskStatus.RUNNING

    async def on_failed(self, failed_task):
        """
        Mark a task and all its dependents as failed, and unblock any watcher.  
        """

        tasks = set([failed_task])

        while tasks:
            task = tasks.pop()
            task_desc = self._details[task].description

            if task == failed_task:
                logging.error('TASK FAILED: %s [%s]', task_desc, task.hex())
            else:
                logging.error('Propagating failure: %s [%s]', task_desc, task.hex())

            # Mark as failed
            self._status[task] = TaskStatus.FAILED
            # Unblocks watchers
            await self._store.not_found(task)

            # Fails the dependents
            # Note: this can visit a task several time, not a problem
            for subtask in self._dependents[task]:
                tasks.add(subtask)

        # This is not a fatal error to the client, by default processing of
        # messages for other ongoing tasks is still permitted.
        return

    async def on_not_found(self, task):
        """
        Unblocks watchers on NOTFOUND

        The difference with FAIL is that we do not presume that dependents will
        fail. It could happen that a task has been completed normally but the
        result cannot be transfered to the client. In this case, further taks
        may still run succesfully. Also, FAIL replaces a DONE message, while
        NOTFOUND follows a DONE message, so NOTFOUND does not interfere with
        task scheduling as FAIL does.
        """
        await self._store.not_found(task)

        # Non fatal error, message processing must go on
        return

    async def on_illegal(self):
        """Should never happen"""
        assert False
