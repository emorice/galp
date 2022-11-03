"""
Client api.

In contrast with a worker which is written as a standalone process, a client is just an
object that can be created and used as part of a larger program.
"""

import logging
import asyncio
import time

from enum import IntEnum, auto
from collections import defaultdict

import zmq
import zmq.asyncio

from galp import async_utils
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

    async def process_scheduled(self):
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

    async def gather(self, *tasks, return_exceptions=False, timeout=None,
            dry_run=False):
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
        self.protocol.add(tasks)

        try:
            err = await asyncio.wait_for(
                self.run_collection(tasks, return_exceptions=return_exceptions,
                    dry_run=dry_run),
                timeout=timeout)
            if err and not return_exceptions:
                raise TaskFailedError(err)
        except ProtocolEndException:
            pass

        if dry_run:
            return None

        results = []
        failed = None
        for task in tasks:
            try:
                results.append(
                    self.protocol.store.get_native(task.name)
                    )
            except (KeyError, DeserializeError) as exc:
                new_exc = TaskFailedError('Failed to collect task '
                        f'[{task}]')
                new_exc.__cause__ = exc
                failed = new_exc
                results.append(new_exc)
        if failed is None or return_exceptions:
            return results
        raise failed

    # Old name of gather
    collect = gather

    async def run(self, *tasks, return_exceptions=False, timeout=None,
            dry_run=False):
        """
        Shorthand for gather with a more variadic style
        """
        results = await self.gather(*tasks, return_exceptions=return_exceptions,
                timeout=timeout, dry_run=dry_run)

        if results and len(results) == 1:
            return results[0]
        return results

    async def run_collection(self, tasks, return_exceptions, dry_run):
        """
        Processes messages until the collection target is achieved
        """

        # Register the termination command
        def _end(status):
            raise ProtocolEndException(status)


        script = self.protocol.script

        main_command = 'DRYRUN' if dry_run else 'RUN'

        collect = script.collect(
                commands=[script.do_once(main_command, t.name) for t in tasks],
                allow_failures=return_exceptions
                )

        script.callback(
            collect, _end
            )
        self.protocol.schedule_new()

        await async_utils.run(
            self.process_scheduled(),
            self.transport.listen_reply_loop()
            )

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

        # Memory only native+serial cache
        self.store = CacheStack(
            dirpath=None,
            serializer=Serializer())

        # Commands
        self.script = Script(store=self.store)

        # Should be phased out, currently used only with the RUNNING state to
        # prevent re-sending a submit after receiving a DOING notification
        self._status = defaultdict(lambda: TaskStatus.UNKNOWN)

        # Task definitions, either given to add() or collected through STAT/FOUND
        self._tasks = {}

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

        This justs updates the client's state, so it can never block and is a
        sync function.
        """

        oset, cset = set(tasks), set()

        while oset:
            # Get a task
            task_details = oset.pop()
            name = task_details.name
            cset.add(name)

            # Check if already added by a previous call to `add`
            if name in self._tasks:
                continue

            # Add the deps to the open set
            for dep_details in task_details.dependencies:
                dep = dep_details.name
                if dep not in cset:
                    oset.add(dep_details)

            # Store the embedded object if literal task
            if hasattr(task_details, 'literal'):
                self.store.put_native(task_details.handle, task_details.literal)

            # Save the dictionary representation of the task object
            self._tasks[name] = task_details.to_dict(name=True)

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

        if verb == 'STAT':
            if self.script.commands[command_key].is_pending():
                return self.stat(self.route, name)
            return None

        raise NotImplementedError(verb)

    # Custom protocol sender
    # ======================
    # For simplicity these set the route inside them

    def submit_task_by_name(self, task_name):
        """Loads task dicts, handles literals and subtasks, and manage stats"""

        # Task dict was added by a stat call
        task_dict = self._tasks.get(task_name)
        if task_dict:
            if 'step_name' in task_dict:
                self.submitted_count[task_name] += 1
                return self.submit(self.route, task_dict)
            # Literals and SubTasks are always satisfied, and never have
            # recursive children
            return self.on_done(None, task_name, task_dict, [])

        raise ValueError(f"Task {task_name.hex()} is unknown")

    def get(self, route, name):
        """
        Send GET for task if not locally available
        """
        children = None
        try:
            children = self.store.get_children(name)
        except KeyError:
            # Not found, send a normal GET
            return super().get(route, name)

        # Found, mark command as done and pass on children
        self.script.commands['GET', name].done(children)
        # Schedule downstream
        self.schedule_new()
        # Supress normal output, removing task from queue
        return None

    # Protocol callbacks
    # ==================
    def on_get(self, route, name):
        try:
            reply = self.put(route, name, self.store.get_serial(name))
            logging.debug('Client GET on %s', name.hex())
        except KeyError:
            reply = self.not_found(route, name)
            logging.warning('Client missed GET on %s', name)

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

        # Put the parent part
        self.store.put_serial(name, (data, children))

        # Mark as done and sets result
        self.script.commands['GET', name].done(children)
        # Schedule for sub-GETs to be sent in the future
        self.schedule_new()

    def on_done(self, route, name, task_dict, children):
        """Given that done_task just finished, mark it as done, letting the
        command system schedule any dependent ready to be submitted, schedule
        GETs for final tasks, etc.
        """
        self._status[name] = TaskStatus.COMPLETED

        # If it was a remotely defined task, store its definition too
        if name not in self._tasks:
            self._tasks[name] = task_dict

        # trigger downstream commands
        command = self.script.commands.get(('SUBMIT', name))
        if command:
            command.done(children)
            self.schedule_new()

        command = self.script.commands.get(('STAT', name))
        if command:
            # Triplet (is_done, dict, children?)
            command.done((True, task_dict, children))
            self.schedule_new()

    def on_doing(self, route, name):
        """Just updates statistics"""
        self.run_count[name] += 1
        self._status[name] = TaskStatus.RUNNING

    def on_failed(self, route, name):
        """
        Mark a task and all its dependents as failed.
        """
        msg = f'Failed to execute task {self._tasks[name]}, check worker logs'
        logging.error('TASK FAILED: %s', msg)

        # Mark fetch command as failed if pending
        self.script.commands['SUBMIT', name].failed(msg)
        assert not self.script.new_commands

        # This is not a fatal error to the client, by default processing of
        # messages for other ongoing tasks is still permitted.

    def on_not_found(self, route, name):
        """
        Mark a fetch as failed, or a stat as unfruitful. Here no propagation
        logic is neeeded, it's already handled by the command dependencies.
        """

        # Mark GET command as failed if issued
        command = self.script.commands.get(('GET', name))
        if command:
            logging.error('TASK RESULT FETCH FAILED: %s', name)
            command.failed('NOTFOUND')
            assert not self.script.new_commands

        # Mark STAT command as done or failed
        command = self.script.commands.get(('STAT', name))
        if command:
            task_dict = self._tasks.get(name)
            if task_dict:
                # Not found remotely, but still found locally
                command.done((False, task_dict, None))
                self.schedule_new()
            else:
                # Found neither remotely not locally, hard error
                command.failed('NOTFOUND')
                assert not self.script.new_commands

    def on_found(self, route, task_dict):
        """
        Mark STAT as completed
        """
        name = task_dict['name']
        self._tasks[name] = task_dict
        # Triplet (is_done, dict, children?)
        self.script.commands['STAT', name].done((False, task_dict, None))
        self.schedule_new()

    def on_illegal(self, route, reason):
        """Should never happen"""
        raise RuntimeError(f'ILLEGAL recevived: {reason}')

    def schedule_new(self):
        """
        Transfer the command queue to the scheduler
        """
        while self.script.new_commands:
            command_key = self.script.new_commands.popleft()
            verb, _ = command_key
            if verb not in ('GET', 'SUBMIT', 'STAT'):
                continue
            self.schedule(command_key)
