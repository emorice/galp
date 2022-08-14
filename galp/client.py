"""
Client api.

In contrast with a worker which is written as a standalone process, a client is just an
object that can be created and used as part of a larger program.
"""

import logging
import asyncio
import time

from contextlib import asynccontextmanager
from enum import IntEnum, auto
from collections import defaultdict

import zmq
import zmq.asyncio

from galp.graph import ensure_task
from galp.cache import CacheStack
from galp.serializer import Serializer, DeserializeError
from galp.protocol import ProtocolEndException
from galp.reply_protocol import ReplyProtocol
from galp.zmq_async_transport import ZmqAsyncTransport
from galp.command_queue import CommandQueue
from galp.commands import Script

class TaskStatus(IntEnum):
    """
    A sorted enum representing the status of a task
    """
    UNKNOWN = auto()

    # DOING received
    RUNNING = auto()

    # DONE received or equiv.
    COMPLETED = auto()

    # FAILED received or equiv.
    FAILED = auto()

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
        self.protocol = BrokerProtocol(
            'BK', router=False,
            schedule=self.schedule # callback to add tasks to the scheduling queue
            )

        self.transport = ZmqAsyncTransport(
            protocol=self.protocol,
            # pylint: disable=no-member # False positive
            endpoint=endpoint, socket_type=zmq.DEALER
            )

        # Request queue
        self.command_queue = CommandQueue()
        self.new_command = asyncio.Event()

    @asynccontextmanager
    async def process_scheduled(self):
        """
        Async context manager running process_scheduled in the background and
        cancelling it at the end.

        Meant to ensure the scheduling loop is correctly cancelled even on
        exceptions, cancellations or timeouts
        """
        scheduler = asyncio.create_task(self._process_scheduled())
        try:
            yield
        finally:
            logging.info('Stopping outgoing message scheduler')
            scheduler.cancel()
            try:
                await scheduler
            except asyncio.CancelledError:
                pass

    async def _process_scheduled(self):
        """
        Send submit/get requests from the queue.

        Receives information from nowhere but the queue, so has to be cancelled
        externally.
        """
        while True:
            self.new_command.clear()
            next_command, next_time = self.command_queue.pop()
            if next_command:
                logging.debug('SCHED: Ready command %s %s', *next_command)
                next_msg = self.protocol.write_next(next_command)
                if next_msg:
                    await self.transport.send_message(next_msg)
                    self.command_queue.requeue(next_command)
                    logging.debug('SCHED: Sent message, requeuing %s %s', *next_command)
                else:
                    logging.debug('SCHED: No message, dropping %s %s', *next_command)

            elif next_time:
                # Wait for either a new command or the end of timeout
                logging.debug('SCHED: No ready command, waiting %s', next_time - time.time())
                try:
                    await asyncio.wait_for(
                        self.new_command.wait(),
                        timeout=next_time - time.time())
                    logging.debug('SCHED: new command signal')
                except asyncio.TimeoutError:
                    logging.debug('SCHED: time elapsed')
            else:
                logging.debug('SCHED: No ready command, waiting forever')
                await self.new_command.wait()

    def schedule(self, command_key):
        """
        Add a task to the scheduling queue.

        This asks the processing queue to consider the task at a later point to
        submit or collect it depending on its state.
        """

        logging.debug("Scheduling %s %s", *command_key)
        self.command_queue.enqueue(command_key)
        self.new_command.set()

    async def gather(self, *tasks, return_exceptions=False, timeout=None):
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

        tasks = list(map(ensure_task, tasks))

        # Update the task graph
        new_inputs = self.protocol.add(tasks)

        # Schedule SUBMITs for all possibly new and ready downstream tasks
        # This should be handled by the Script
        for task in new_inputs:
            self.schedule(('SUBMIT', task))

        try:
            err = await asyncio.wait_for(
                self.run_collection(tasks, return_exceptions=return_exceptions),
                timeout=timeout)
            if err and not return_exceptions:
                raise TaskFailedError(err)
        except ProtocolEndException:
            pass

        results = []
        failed = None
        for task in tasks:
            try:
                results.append(
                    self.protocol.store.get_native(task.name)
                    )
            except (KeyError, DeserializeError) as exc:
                new_exc = TaskFailedError(task.name)
                new_exc.__cause__ = exc
                failed = new_exc
                results.append(new_exc)
        if failed is None or return_exceptions:
            return results
        raise failed

    # Old name of gather
    collect = gather

    async def run(self, *tasks, return_exceptions=False, timeout=None):
        """
        Shorthand for gather with a more variadic style
        """
        results = await self.gather(*tasks, return_exceptions=return_exceptions, timeout=timeout)

        if len(results) == 1:
            return results[0]
        return results

    async def run_collection(self, tasks, return_exceptions):
        """
        Processes messages until the collection target is achieved
        """

        # Schedule GETs for tasks that were already done in a previous
        # collection run
        self.protocol.get_done(tasks)

        # Register the termination command
        def _end(status):
            raise ProtocolEndException(status)

        init_messages = []

        collect = self.protocol.script.collect(
                parent=None,
                names=[t.name for t in tasks],
                out=init_messages,
                allow_failures=return_exceptions
                )
        self.protocol.script.callback(
            collect, _end
            )

        logging.info('CMD REP: %s', init_messages)

        async with self.process_scheduled():
            await self.transport.listen_reply_loop()

        return collect.result

class BrokerProtocol(ReplyProtocol):
    """
    Main logic of the interaction of a client with a broker
    """
    def __init__(self, name, router, schedule):
        super().__init__(name, router)

        self.schedule = schedule

        # With a DEALER socket, we send messages with no routing information by
        # default.
        self.route = self.default_route()


        ## Replacement for status tracker
        # repo of keys, only for prototyping
        self._command_keys = {}
        # command defs
        self.script = Script()

        ## Current task status
        self._status = defaultdict(lambda: TaskStatus.UNKNOWN)

        self._details = {}
        self._dependents = defaultdict(set)
        self._dependencies = defaultdict(set)

        # Memory only native+serial cache
        self.store = CacheStack(
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
        idempotent way.

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

    def get_done(self, tasks):
        """
        Schedule for collection any new task that was already done.
        """

        for task_details in tasks:
            get_key = 'GET', task_details.name
            if (
                get_key not in self.script.commands
                and
                self._status[task_details.name] is TaskStatus.COMPLETED
                ):
                logging.debug("Scheduling extra GET for upgraded %s",
                    task_details.name.hex())
                self.schedule(get_key)

    def write_next(self, command_key):
        """
        Returns the next nessage to be sent for a task given the information we
        have about it
        """
        verb, name = command_key

        if verb == 'SUBMIT':
            if self._status[name] < TaskStatus.RUNNING:
                return self.submit_task_by_name(name)
            return None

        if verb == 'GET':
            if self.script.commands[command_key].is_pending():
                return self.get(self.route, name)
            return None

        raise NotImplementedError(verb)

    # Custom protocol sender
    # ======================
    # For simplicity these set the route inside them

    def submit_task_by_name(self, task_name):
        """Loads details, handles literals and subtasks, and manage stats"""
        if task_name not in self._details:
            raise ValueError(f"Task {task_name.hex()} is unknown")
        details = self._details[task_name]

        if hasattr(details, 'literal'):
            # Reference the literal
            self.store.put_native(details.handle, details.literal)
            # Mark any possible GET as completed
            command = self.script.commands.get(('GET', task_name))
            if command:
                cmd_rep = command.done([dep.name for dep in details.dependencies])
                logging.info('CMD REP: %s', cmd_rep)
                for command_key in cmd_rep:
                    self.schedule(command_key)
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
            reply = self.put(route, name, self.store.get_serial(name))
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

        data, children = serialized

        # Schedule sub-gets if necessary
        # To be moved to the script engine eventually
        # Also we should get rid of the handle
        for child_name in children:
            # We do not send explicit DONEs for subtasks, so we mark them as
            # done when we receive the parent data.
            self._status[child_name] = TaskStatus.COMPLETED
            # Schedule it for GET to be sent in the future
            self.schedule(('GET', child_name))

        # Put the parent part
        self.store.put_serial(name, (data, children))

        # Fetch command
        command = self.script.commands['GET', name]
        # Mark as done and sets result
        command.done(children)

    def on_done(self, route, name):
        """Given that done_task just finished, mark it as done, schedule any
        dependent ready to be submitted, and schedule GETs for final tasks.
        """

        self._status[name] = TaskStatus.COMPLETED

        # check if we have ready pending outputs, if yes schedule them
        for dept in self._dependents[name]:
            if all(
                self._status[sister_dep] >= TaskStatus.COMPLETED
                for sister_dep in self._dependencies[dept]
                ):
                self.schedule(('SUBMIT', dept))

        # check if we have a pending get, if yes schedule it
        command_key = 'GET', name
        command = self.script.commands.get(command_key)
        if command:
            self.schedule(command_key)

    def on_doing(self, route, name):
        """Just updates statistics"""
        self.run_count[name] += 1
        self._status[name] = TaskStatus.RUNNING

    def on_failed(self, route, name):
        """
        Mark a task and all its dependents as failed.
        """

        tasks = set([name])

        desc = self._details[name].description
        msg = f'Failed to execute task {desc} [{name}], check worker logs'

        while tasks:
            task = tasks.pop()
            task_desc = self._details[task].description

            if task == name:
                logging.error('TASK FAILED: %s [%s]', task_desc, task)
            else:
                logging.error('Propagating failure: %s [%s]', task_desc, task)

            # Mark as failed
            self._status[task] = TaskStatus.FAILED
            # Mark fetch command as failed if pending:
            command = self.script.commands.get(('GET', task))
            if command:
                command.failed(msg)

            # Fail the dependents
            # Note: this can visit a task several time, not a problem
            for subtask in self._dependents[task]:
                tasks.add(subtask)

        # This is not a fatal error to the client, by default processing of
        # messages for other ongoing tasks is still permitted.

    def on_not_found(self, route, name):
        """
        Mark a fetch as failed. Here no propagation logic is neeeded, it's
        already handled by the command dependencies.
        """

        task_desc = self._details[name].description
        logging.error('TASK RESULT FETCH FAILED: %s [%s]', task_desc, name)
        # Mark fetch command as failed
        command = self.script.commands.get(('GET', name))
        command.failed('NOTFOUND')

    def on_illegal(self, route, reason):
        """Should never happen"""
        raise RuntimeError(f'ILLEGAL recevived: {reason}')
