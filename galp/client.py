"""
Client api.

In contrast with a worker which is written as a standalone process, a client is just an
object that can be created and used as part of a larger program.
"""

import logging
import asyncio

from enum import IntEnum, auto
from collections import defaultdict

import zmq
import zmq.asyncio

from galp.cache import CacheStack
from galp.serializer import Serializer, DeserializeError
from galp.protocol import Protocol
from galp.zmq_async_transport import ZmqAsyncTransport
from galp.store import Store
from galp.graph import Handle


class TaskStatus(IntEnum):
    """
    A sorted enum representing the status of a task
    """
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
    """
    Error thrown in the client if a task failed for any reason.

    Does not contain propagated error information yet.
    """

class Client:
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

        self.proto = BrokerProtocol(
            'BK', router=False,
            schedule=self.schedule # callback to add tasks to the scheduling queue
            )

        self.transport = ZmqAsyncTransport(
            protocol=self.proto,
            # pylint: disable=no-member # False positive
            endpoint=endpoint, socket_type=zmq.DEALER
            )

        # Request queue
        self._scheduled = asyncio.Queue()

    async def process_scheduled(self):
        """
        Send submit/get requests from the queue.

        Receives information from nowhere but the queue, so has to be cancelled
        externally.
        """

        while True:
            name = await self._scheduled.get()
            next_msg = self.proto.write_next(name)
            if next_msg is not None:
                await self.transport.send_message(next_msg)

    def schedule(self, task_name):
        """
        Add a task to the scheduling queue.

        This asks the processing queue to consider the task at a later point to
        submit or collect it depending on its state.

        This uses an asyncio queue but we assume it's unbounded and putting is
        synchronous.
        """

        logging.debug("Scheduling %s", task_name.hex())
        self._scheduled.put_nowait(task_name)

    async def collect(self, *tasks, return_exceptions=False, timeout=None):
        """
        Recursively submit the tasks, wait for completion, fetches, deserialize
        and returns the actual results.

        Written as a coroutine, so that it can be called inside an asynchronous
        application, such as a worker or proxy or whatever: the caller is in
        charge of the event loop.

        Args:
            return_exceptions: if False, the collection is interrupted and an
                exception is raised as soon as we know any result will not be
                obtained due to failures. If True, keep running until all
                required tasks are either done or failed.
        """


        # Update our state
        new_inputs = self.proto.add(tasks)

        # Schedule SUBMITs for all possibly new and ready downstream tasks
        for task in new_inputs:
            self.schedule(task)

        # Update the list of final tasks and schedule GETs for the already done
        # Note: comes after submit as derived task need to be submitted first to
        # be marked as completed before.
        # Note 2: the above note do not work with async scheduling and should not
        # be relied on
        self.proto.add_finals(tasks)

        scheduler = asyncio.create_task(self.process_scheduled())

        await self.transport.listen_reply_loop()

        scheduler.cancel()

        try:
            await scheduler
        except asyncio.CancelledError:
            pass

class BrokerProtocol(Protocol):
    """
    Main logic of the interaction of a client with a broker
    """
    def __init__(self, name, router, schedule):
        super().__init__(name, router)

        self.schedule = schedule

        # With a DEALER socket, we send messages with no routing information by
        # default.
        self.route = self.default_route()

        ## Current task status
        self._status = defaultdict(lambda: TaskStatus.UNKNOWN)

        # Tasks whose result is to be fetched
        self._finals = set()

        self._details = {}
        self._dependents = defaultdict(set)
        self._dependencies = defaultdict(set)

        # Memory only native+serial cache
        self._cache = CacheStack(
            dirpath=None,
            serializer=Serializer())

        # Public attributes: counters for the number of SUBMITs sent and DOING
        # received for each task
        # Used for reporting and testing cache behavior
        self.submitted_count = defaultdict(int)
        self.run_count = defaultdict(int)


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

        # Also build the inverse dependencies lookup table, i.e the 'children' task.
        # child/parent can be ambiguous here (what is the direction of the
        # arcs ?) so we call them "dependents" as opposed to "dependencies"

        oset, cset = set(tasks), set()

        new_top_level = set()
        while oset:
            # Get a task
            task_details = oset.pop()
            task = task_details.name
            cset.add(task)

            # Check if already added by a previous call to `add`
            if task in self._details:
                continue

            # Add the links
            for dep_details in task_details.dependencies:
                dep = dep_details.name
                self._dependencies[task].add(dep)
                self._dependents[dep].add(task)
                if dep not in cset:
                    oset.add(dep_details)

            # Save details, and mark as seen
            self._details[task] = task_details

            # Check unseen input tasks
            # Note: True if no dependencies at all
            if all(
                    self._status[dep] >= TaskStatus.COMPLETED
                    for dep in self._dependencies[task]
                ):
                new_top_level.add(task)

        return new_top_level

    def add_finals(self, tasks):
        """
        Add tasks to the set of finals (= to be collected) tasks, and schedule
        for collection any new one that was already done.
        """

        for task_details in tasks:
            if (
                task_details not in self._finals
                and
                self._status[task_details.name] is TaskStatus.COMPLETED
                ):
                logging.debug("Scheduling extra GET for upgraded %s",
                    task_details.name.hex())
                self.schedule(task_details.name)
            self._finals.add(task_details.name)

    def write_next(self, name):
        """
        Returns the next nessage to be sent for a task given the information we
        have about it
        """
        if self._status[name] < TaskStatus.COMPLETED:
            return self.submit_task_by_name(name)

        if name in self._finals:
            return self.get(self.route, name)

        return None

    # Custom protocol sender
    # ======================
    # For simplicity these set the route inside them

    def submit_task_by_name(self, task_name):
        """Loads details, handles hereis and subtasks, and manage stats"""
        if task_name not in self._details:
            raise ValueError(f"Task {task_name.hex()} is unknown")
        details = self._details[task_name]

        self._status[task_name] = TaskStatus.SUBMITTED

        if hasattr(details, 'hereis'):
            self._cache.put_native(details.handle, details.hereis)
            return self.on_done(None, task_name)

        # Sub-tasks are automatically satisfied when their parent and unique
        # dependency is
        if hasattr(details, 'parent'):
            return self.on_done(None, task_name)

        self.submitted_count[task_name] += 1
        return self.submit_task(self.route, details)

    # Protocol callbacks
    # ==================
    def on_get(self, route, name):
        try:
            reply = self.put(route, name, self._cache.get_serial(name))
            logging.debug('Client GET on %s', name.hex())
        except KeyError:
            reply = self.not_found(route, name)
            logging.warning('Client missed GET on %s', name.hex())

        return reply

    def on_put(self, route, name, serialized):
        """
        Receive data, and schedule sub-gets if necessary and check for
        termination
        """

        handle = (
            self._details[name].handle
            if name in self._details
            else Handle(name)
            )

        proto, data, children = serialized

        # Schedule sub-gets if necessary
        for i in range(children):
            children_name = handle[i].name
            # We do not send explicit DONEs for subtasks, so we mark them as
            # done when we receive the parent data.
            self._status[children_name] = TaskStatus.COMPLETED
            # The scheduler needs to know that this task wants to be fetched
            self._finals.add(children_name)
            # Schedule it for GET to be sent in the future
            self.schedule(children_name)

        # Put the parent part
        self._cache.put_serial(name, proto, data, children.to_bytes(1, 'big'))

        # Mark the task as available, not that this do not imply the
        # availability of the sub-tasks
        self._status[name] = TaskStatus.AVAILABLE

    def on_done(self, route, name):
        """Given that done_task just finished, mark it as done, schedule any
        dependent ready to be submitted, and schedule GETs for final tasks.

        """

        self._status[name] = TaskStatus.COMPLETED

        if name in self._finals:
            self.schedule(name)

        for dept in self._dependents[name]:
            if all(
                self._status[sister_dep] >= TaskStatus.COMPLETED
                for sister_dep in self._dependencies[dept]
                ):
                self.schedule(dept)

    def on_doing(self, route, name):
        """Just updates statistics"""
        self.run_count[name] += 1
        self._status[name] = TaskStatus.RUNNING

    def on_failed(self, route, name):
        """
        Mark a task and all its dependents as failed.
        """

        tasks = set([name])

        while tasks:
            task = tasks.pop()
            task_desc = self._details[task].description

            if task == name:
                logging.error('TASK FAILED: %s [%s]', task_desc, task.hex())
            else:
                logging.error('Propagating failure: %s [%s]', task_desc, task.hex())

            # Mark as failed
            self._status[task] = TaskStatus.FAILED

            # Fails the dependents
            # Note: this can visit a task several time, not a problem
            for subtask in self._dependents[task]:
                tasks.add(subtask)

        # This is not a fatal error to the client, by default processing of
        # messages for other ongoing tasks is still permitted.

    def on_illegal(self, route):
        """Should never happen"""
        assert False
