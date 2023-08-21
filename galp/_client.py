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
from typing import Callable, Iterable, Any

import zmq
import zmq.asyncio

import galp.messages as gm
import galp.commands as cm
import galp.task_types as gtt

from galp import async_utils
from galp.cache import CacheStack
from galp.serializer import DeserializeError
from galp.protocol import ProtocolEndException, RoutedMessage
from galp.reply_protocol import ReplyProtocol
from galp.zmq_async_transport import ZmqAsyncTransport
from galp.command_queue import CommandQueue
from galp.query import run_task
from galp.task_types import (TaskName, TaskNode, LiteralTaskDef, QueryTaskDef,
    ensure_task_node, TaskSerializer)

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

    def __init__(self, endpoint: str):
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

    async def process_scheduled(self) -> None:
        """
        Send submit/get requests from the queue.

        Receives information from nowhere but the queue, so has to be cancelled
        externally.
        """
        while True:
            self.new_command.clear()
            next_command, next_time = self.command_queue.pop()
            if next_command:
                logging.debug('SCHED: Ready command %s %s', *next_command.key)
                next_msg = self.protocol.write_next(next_command)
                if next_msg:
                    await self.transport.send_message(next_msg)
                    self.command_queue.requeue(next_command)
                    logging.debug('SCHED: Sent message, requeuing %s %s',
                            *next_command.key)
                else:
                    logging.debug('SCHED: No message, dropping %s %s',
                            *next_command.key)

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

    def schedule(self, command: cm.InertCommand) -> None:
        """
        Add a task to the scheduling queue.

        This asks the processing queue to consider the task at a later point to
        submit or collect it depending on its state.
        """

        logging.debug("Scheduling %s %s", *command.key)
        self.command_queue.enqueue(command)
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

        val = await asyncio.wait_for(
            self.run_collection(task_nodes, return_exceptions=return_exceptions,
                dry_run=dry_run),
            timeout=timeout)

        if isinstance(val, cm.Failed):
            if not return_exceptions:
                raise TaskFailedError(val.error)
            # Issue #83: actually implement this
            cmd_results = [val.error] * len(task_nodes)
        else:
            cmd_results = val.result

        if dry_run:
            return None

        results: list[Any] = []
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
            return_exceptions: bool, dry_run: bool) -> cm.FinalResult[list, str]:
        """
        Processes messages until the collection target is achieved
        """

        # Register the termination command
        def _end(value):
            raise ProtocolEndException(value)

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
            _, cmds = script.callback(collect, _end)
            self.protocol.schedule_new(cmds)

            await async_utils.run(
                self.process_scheduled(),
                self.transport.listen_reply_loop()
                )
        except ProtocolEndException:
            pass
        # Issue 84: this work because End is only raised after collect is done,
        # but that's bad style.
        assert not isinstance(collect.val, cm.Pending)
        return collect.val

class BrokerProtocol(ReplyProtocol):
    """
    Main logic of the interaction of a client with a broker
    """
    def __init__(self, name: str, router: bool,
            schedule: Callable[[cm.InertCommand], None]):
        super().__init__(name, router)

        self.schedule = schedule

        # Memory only native+serial cache
        self.store = CacheStack(
            dirpath=None,
            serializer=TaskSerializer)

        # Commands
        self.script = cm.Script(store=self.store)

        # Should be phased out, currently used only with the RUNNING state to
        # prevent re-sending a submit after receiving a DOING notification
        self._status : defaultdict[TaskName, TaskStatus] = defaultdict(lambda: TaskStatus.UNKNOWN)

        # Public attributes: counters for the number of SUBMITs sent and DOING
        # received for each task
        # Used for reporting and testing cache behavior
        self.submitted_count : defaultdict[TaskName, int] = defaultdict(int)
        self.run_count : defaultdict[TaskName, int] = defaultdict(int)

        # Resources
        self.resources = gtt.Resources(cpus=1)


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

    def write_next(self, command: cm.InertCommand) -> RoutedMessage | None:
        """
        Returns the next nessage to be sent for a task given the information we
        have about it

        (Sorry for the None disjunction, this is in the middle of the part where
        we supress messages whose result is already known)
        """
        if not command.is_pending():
            # Promise has been fulfilled since before reaching here
            return None

        match command:
            case cm.Submit():
                name = command.task_def.name
                if self._status[name] < TaskStatus.RUNNING:
                    self.submitted_count[name] += 1
                    sub = gm.Submit(task_def=command.task_def,
                                    resources=self.get_resources(command.task_def))
                    return self.route_message(None, sub)
            case cm.Get():
                get = self.get(command.name)
                if get is not None:
                    return self.route_message(None, get)
            case cm.Stat():
                return self.route_message(None, gm.Stat(name=command.name))
            case _:
                raise NotImplementedError(command)

        return None

    def get_resources(self, task_def: gtt.CoreTaskDef) -> gtt.Resources:
        """
        Decide how much resources to allocate to a task
        """
        _ = task_def # to be used later
        return self.resources

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

    def get(self, name: TaskName) -> gm.Get | None:
        """
        Send GET for task if not locally available
        """
        try:
            result_ref = self.store.get_children(name)
        except KeyError:
            # Not found, send a normal GET
            return gm.Get(name=name)

        # Found, mark command as done and pass on children
        self.schedule_new(
            self.script.commands['GET', name].done(result_ref.children)
            )
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
        for child in msg.children:
            # We do not send explicit DONEs for subtasks, so we mark them as
            # done when we receive the parent data.
            self._status[child.name] = TaskStatus.COMPLETED

        # Put the parent part
        self.store.put_serial(msg.name, (msg.data, msg.children))

        # Mark as done and sets result
        self.schedule_new(
            self.script.commands['GET', msg.name].done(msg.children)
            )

    def on_done(self, msg: gm.Done) -> None:
        """Given that done_task just finished, mark it as done, letting the
        command system schedule any dependent ready to be submitted, schedule
        GETs for final tasks, etc.
        """
        task_def = msg.task_def
        name = task_def.name
        self._status[name] = TaskStatus.COMPLETED

        # trigger downstream commands
        command = self.script.commands.get(('SUBMIT', name))
        if command:
            self.schedule_new(
                command.done(msg.result)
                )

        command = self.script.commands.get(('STAT', name))
        if command:
            self.schedule_new(
                command.done(msg)
                )

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
        reps = self.script.commands['SUBMIT', task_def.name].failed(err_msg)
        assert not reps

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
            reps = command.failed('NOTFOUND')
            assert not reps

        # Mark STAT command as done
        command = self.script.commands.get(('STAT', name))
        if command:
            self.schedule_new(
                command.done(msg)
                )

    def on_found(self, msg: gm.Found):
        """
        Mark STAT as completed
        """
        task_def = msg.task_def
        name = task_def.name

        # Mark STAT command as done
        command = self.script.commands.get(('STAT', name))
        if command:
            self.schedule_new(
                command.done(msg)
                )

    def on_illegal(self, msg: gm.Illegal):
        """Should never happen"""
        raise RuntimeError(f'ILLEGAL recevived: {msg.reason}')

    def schedule_new(self, commands: Iterable[cm.InertCommand]) -> None:
        """
        Transfer the command queue to the scheduler
        """
        for command in commands:
            self.schedule(command)
