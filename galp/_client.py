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
from typing import Callable

import zmq
import zmq.asyncio

import galp.messages as gm

from galp import async_utils
from galp.graph import ensure_task_node
from galp.cache import CacheStack
from galp.serializer import Serializer, DeserializeError
from galp.protocol import ProtocolEndException, RoutedMessage, Replies
from galp.reply_protocol import ReplyProtocol
from galp.zmq_async_transport import ZmqAsyncTransport
from galp.command_queue import CommandQueue
from galp.commands import Script
from galp.query import run_task
from galp.task_types import (TaskName, TaskNode, LiteralTaskDef, TaskDef,
        QueryTaskDef, CoreTaskDef)

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

    async def gather(self, *tasks, return_exceptions: bool = False, timeout=None,
            dry_run: bool = False):
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

        task_nodes = list(map(ensure_task_node, tasks))

        # Update the task graph
        self.protocol.add(task_nodes)

        err, cmd_results = await asyncio.wait_for(
            self.run_collection(task_nodes, return_exceptions=return_exceptions,
                dry_run=dry_run),
            timeout=timeout)

        if err and not return_exceptions:
            raise TaskFailedError(cmd_results)

        if dry_run:
            return None

        results = []
        failed = None
        for task, cmd_result in zip(task_nodes, cmd_results):
            tdef = task.task_def
            if isinstance(tdef, QueryTaskDef):
                # For queries, result is inline
                results.append(cmd_result)
            else:
                # For the rest, result is in store on success
                try:
                    results.append(
                        self.protocol.store.get_native(task.name)
                        )
                # On failure, more details on the error can be provided inline
                except (KeyError, DeserializeError) as exc:
                    new_exc = TaskFailedError('Failed to collect task '
                            f'[{task.task_def}]: {cmd_result}')
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

    async def run_collection(self, tasks: list[TaskNode],
            return_exceptions: bool, dry_run: bool):
        """
        Processes messages until the collection target is achieved
        """

        # Register the termination command
        def _end(status, _result):
            raise ProtocolEndException(status)

        script = self.protocol.script

        # Note: this is shared by different calls to the collection methods. We
        # currently don't have a way to specify that the same task should be
        # considered as either failed or pending depending on the caller. Doing
        # so would require duplicating the command graph. So for now, calling
        # collection routines twice simulatenously on the same client with
        # different keep_going parameters is not allowed and will result in
        # unspecified behavior.
        if return_exceptions:
            script.keep_going = True
        else:
            script.keep_going = False

        collect = script.collect(
                commands=[run_task(script, t, dry=dry_run) for t in tasks]
                )

        try:
            script.callback(
                collect, _end
                )
            self.protocol.schedule_new()

            await async_utils.run(
                self.process_scheduled(),
                self.transport.listen_reply_loop()
                )
        except ProtocolEndException:
            pass
        return collect.status, collect.result

class BrokerProtocol(ReplyProtocol):
    """
    Main logic of the interaction of a client with a broker
    """
    def __init__(self, name: str, router: bool, schedule: Callable):
        super().__init__(name, router)

        self.schedule = schedule

        # Memory only native+serial cache
        self.store = CacheStack(
            dirpath=None,
            serializer=Serializer())

        # Commands
        self.script = Script(store=self.store)

        # Should be phased out, currently used only with the RUNNING state to
        # prevent re-sending a submit after receiving a DOING notification
        self._status : defaultdict[TaskName, TaskStatus] = defaultdict(lambda: TaskStatus.UNKNOWN)

        # Task definitions, either given to add() or collected through STAT/FOUND
        self._tasks : dict[TaskName, TaskDef] = {}

        # Public attributes: counters for the number of SUBMITs sent and DOING
        # received for each task
        # Used for reporting and testing cache behavior
        self.submitted_count : defaultdict[TaskName, int] = defaultdict(int)
        self.run_count : defaultdict[TaskName, int] = defaultdict(int)

    def add(self, tasks: list[TaskNode]) -> None:
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

        oset = {t.name: t for t in tasks}
        cset: set[TaskName] = set()

        while oset:
            # Get a task
            name, task_node = oset.popitem()
            cset.add(name)

            # Check if already added by a previous call to `add`
            if name in self._tasks:
                continue

            # Add the deps to the open set
            for dep_node in task_node.dependencies:
                dep_name = dep_node.name
                if dep_name not in cset:
                    # The graph should be locally generated, and only contain
                    # true tasks not references
                    assert isinstance(dep_node, TaskNode)
                    oset[dep_name] = dep_node

            # Store the embedded object if literal task
            if isinstance(task_node.task_def, LiteralTaskDef):
                self.store.put_native(name, task_node.data)

            # Save the task_definition
            self._tasks[name] = task_node.task_def

    def write_next(self, command_key) -> RoutedMessage | None:
        """
        Returns the next nessage to be sent for a task given the information we
        have about it

        (Sorry for the None disjunction, this is in the middle of the part where
        we supress messages whose result is already known)
        """
        verb, name = command_key

        if verb == 'SUBMIT':
            if self._status[name] < TaskStatus.RUNNING:
                sub = self.submit_task_by_name(name)
                if sub is not None:
                    return self.route_message(None, sub)
            return None

        if verb == 'GET':
            if self.script.commands[command_key].is_pending():
                get = self.get(name)
                if get is not None:
                    return self.route_message(None, get)
            return None

        if verb == 'STAT':
            if self.script.commands[command_key].is_pending():
                return self.route_message(None, gm.Stat(name=name))
            return None

        raise NotImplementedError(verb)

    # Custom protocol sender
    # ======================
    # For simplicity these set the route inside them

    def route_message(self, orig: RoutedMessage | None, new: gm.Message | RoutedMessage):
        """
        Send message back to original sender only for replies to GETs

        Everything else is default-addressed to the broker
        """
        if isinstance(new, gm.Put | gm.NotFound):
            assert orig is not None
            return orig.reply(new)
        return super().route_message(orig, new)

    def submit_task_by_name(self, task_name: TaskName) -> gm.Submit | None:
        """Loads task dicts, handles literals and subtasks, and manage stats"""

        # Task dict was added by a stat call
        task_def = self._tasks.get(task_name)
        if task_def is None:
            raise ValueError(f"Task {task_name.hex()} is unknown")
        if isinstance(task_def, CoreTaskDef):
            self.submitted_count[task_name] += 1
            return gm.Submit(task_def=task_def)
        # Literals and SubTasks are always satisfied, and never have
        # recursive children
        self.on_done(gm.Done(task_def=task_def, children=[]))
        return None

    def get(self, name: TaskName) -> gm.Get | None:
        """
        Send GET for task if not locally available
        """
        children = None
        try:
            children = self.store.get_children(name)
        except KeyError:
            # Not found, send a normal GET
            return gm.Get(name=name)

        # Found, mark command as done and pass on children
        self.script.commands['GET', name].done(children)
        # Schedule downstream
        self.schedule_new()
        # Supress normal output, removing task from queue
        return None

    # Protocol callbacks
    # ==================
    def on_get(self, msg: gm.Get):
        """
        Sends back Put or NotFound
        """
        name = msg.name
        reply: gm.Put | gm.NotFound
        try:
            data, children = self.store.get_serial(name)
            reply = gm.Put(name=name, data=data, children=children)
            logging.debug('Client GET on %s', name.hex())
        except KeyError:
            reply = gm.NotFound(name=name)
            logging.warning('Client missed GET on %s', name)

        return reply

    def on_put(self, msg: gm.Put):
        """
        Receive data, and schedule sub-gets if necessary and check for
        termination
        """
        # Schedule sub-gets if necessary
        # To be moved to the script engine eventually
        # Also we should get rid of the handle
        for child_name in msg.children:
            # We do not send explicit DONEs for subtasks, so we mark them as
            # done when we receive the parent data.
            self._status[child_name] = TaskStatus.COMPLETED

        # Put the parent part
        self.store.put_serial(msg.name, (msg.data, msg.children))

        # Mark as done and sets result
        self.script.commands['GET', msg.name].done(msg.children)
        # Schedule for sub-GETs to be sent in the future
        self.schedule_new()

    def on_done(self, msg: gm.Done) -> None:
        """Given that done_task just finished, mark it as done, letting the
        command system schedule any dependent ready to be submitted, schedule
        GETs for final tasks, etc.
        """
        task_def = msg.task_def
        name = task_def.name
        self._status[name] = TaskStatus.COMPLETED

        # If it was a remotely defined task, store its definition too
        if name not in self._tasks:
            self._tasks[name] = task_def

        # trigger downstream commands
        command = self.script.commands.get(('SUBMIT', name))
        if command:
            command.done(msg.children)
            self.schedule_new()

        command = self.script.commands.get(('STAT', name))
        if command:
            # Triplet (is_done, dict, children?)
            command.done((True, task_def, msg.children))
            self.schedule_new()

        return None

    def on_doing(self, msg: gm.Doing):
        """
        Just updates statistics.

        This has the side effect of preventing further submit retries to be
        sent.
        """
        self.run_count[msg.name] += 1
        self._status[msg.name] = TaskStatus.RUNNING

    def on_failed(self, msg: gm.Failed):
        """
        Mark a task and all its dependents as failed.
        """
        task_def = msg.task_def
        err_msg = f'Failed to execute task {task_def}, check worker logs'
        logging.error('TASK FAILED: %s', err_msg)

        # Mark fetch command as failed if pending
        self.script.commands['SUBMIT', task_def.name].failed(err_msg)
        assert not self.script.new_commands

        # This is not a fatal error to the client, by default processing of
        # messages for other ongoing tasks is still permitted.

    def on_not_found(self, msg: gm.NotFound):
        """
        Mark a fetch as failed, or a stat as unfruitful. Here no propagation
        logic is neeeded, it's already handled by the command dependencies.
        """

        name = msg.name

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

    def on_found(self, msg: gm.Found):
        """
        Mark STAT as completed
        """
        task_def = msg.task_def
        name = task_def.name
        self._tasks[name] = task_def
        # Triplet (is_done, dict, children?)
        self.script.commands['STAT', name].done((False, task_def, None))
        self.schedule_new()

    def on_illegal(self, msg: gm.Illegal):
        """Should never happen"""
        raise RuntimeError(f'ILLEGAL recevived: {msg.reason}')

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
