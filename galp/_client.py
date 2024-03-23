"""
Client api.

In contrast with a worker which is written as a standalone process, a client is just an
object that can be created and used as part of a larger program.
"""

import asyncio
import logging

from collections import defaultdict
from typing import Iterable, Any

import zmq
import zmq.asyncio

import galp.net.core.types as gm
import galp.commands as cm
import galp.task_types as gtt

from galp.result import Result, Ok, Error
from galp.cache import CacheStack
from galp.protocol import (ProtocolEndException, make_stack,
    TransportMessage, Writer)
from galp.zmq_async_transport import ZmqAsyncTransport
from galp.control_queue import ControlQueue
from galp.query import run_task
from galp.asyn import filter_commands

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
        cpus_per_task: number of cpus *per task* to allocate
    """

    def __init__(self, endpoint: str, cpus_per_task: int | None = None):
        # Public attribute: counter for the number of SUBMITs for each task
        # Used for reporting and testing cache behavior
        self.submitted_count : defaultdict[gtt.TaskName, int] = defaultdict(int)

        # State keeping:
        # Literals to be exposed on the network
        self._store = CacheStack(
            dirpath=None,
            serializer=gtt.TaskSerializer)
        # Pending requests
        self._command_queue: ControlQueue[cm.InertCommand] = ControlQueue()
        # Async graph
        self._script = cm.Script()
        # Misc param
        self._cpus_per_task = cpus_per_task or 1

        # Communication
        def on_message(_write: Writer[gm.Message], msg: gm.Message
                ) -> Iterable[TransportMessage] | Error:
            match msg:
                case gm.Reply():
                    return self._schedule_new(
                        self._script.done(msg.request, msg.value)
                        )
                case gm.NextRequest():
                    command = self._command_queue.on_next_item()
                    return ([] if command is None
                            else [self._write_next(command)]
                            )
                case _:
                    return Error(f'Unexpected {msg}')
        self._stack = make_stack(on_message, name='BK')
        self._transport = ZmqAsyncTransport(
            stack=self._stack, endpoint=endpoint, socket_type=zmq.DEALER
            )

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

        task_nodes = list(map(gtt.ensure_task_node, tasks))

        # Populate the store
        store_literals(self._store, task_nodes)

        cmd_vals = await asyncio.wait_for(
            self._run_collection(task_nodes, cm.ExecOptions(
                    keep_going=return_exceptions, dry=dry_run,
                    resources=gtt.ResourceClaim(cpus=self._cpus_per_task)
                    )),
            timeout=timeout)

        results: list[Any] = []
        for val in cmd_vals:
            match val:
                case Ok():
                    results.append(val.value)
                case Error():
                    exc = TaskFailedError(
                        f'Failed to collect task: {val.error}'
                        )
                    if return_exceptions:
                        results.append(exc)
                    else:
                        raise exc
                case _: # Pending()
                    # This should only be found if keep_going is False and an
                    # other command fails, so we should find a Failed and raise
                    # without actually returning this one.
                    # Fill it in anyway, to avoid confusions
                    results.append(val)

        # Conventional result for dry runs, as results would then only contain
        # internal objects that shouldn't be exposed
        # In the future, we could instead return some dry-run information for
        # programmatic use
        if dry_run:
            return None

        return results

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

    async def _run_collection(self, tasks: list[gtt.TaskNode],
                              exec_options: cm.ExecOptions) -> list[Result]:
        """
        Processes messages until the collection target is achieved
        """
        commands = [run_task(t, exec_options) for t in tasks]
        collect = self._script.collect(commands, exec_options.keep_going)
        end: cm.Command = collect.eventually(cm.End)

        try:
            primitives = self._script.init_command(end)
            await self._transport.send_messages(
                    self._schedule_new(primitives)
                    )
            err = await self._transport.listen_reply_loop()
            if err is not None:
                logging.error('Communication error: %s', err)
        except ProtocolEndException:
            pass
        # Issue 84: this work because End is only raised after collect is done,
        # but that's bad style.
        return [c.val for c in commands]

    def _write_next(self, command: cm.InertCommand) -> TransportMessage:
        """
        Returns the next nessage to be sent for a task given the information we
        have about it.
        """
        assert command.is_pending()

        match command:
            case cm.Send():
                match command.request:
                    case gm.Submit(task_def):
                        self.submitted_count[task_def.name] += 1
                return self._stack.write_local(command.request)
            case _:
                raise NotImplementedError(command)

    def _filter_local_get(self, command: cm.InertCommand
                         ) -> tuple[list[cm.InertCommand], list[cm.InertCommand]]:
        """
        Fulfill GETs that are locally available
        """
        # Not a Get, leave as-is
        match command:
            case cm.Send():
                match command.request:
                    case gm.Get():
                        name = command.request.name
                    case _:
                        return [command], []
            case cm.End():
                raise ProtocolEndException(command.value)
            case _:
                return [command], []

        try:
            res = self._store.get_serial(name)
        except KeyError:
            # Not found, leave as-is
            return [command], []

        assert False, 'Deprecated store use'
        # Found, mark command as done and pass on children
        return [], self._script.done(command.key, Ok(res))

    def _schedule_new(self, commands: Iterable[cm.InertCommand]
            ) -> list[TransportMessage]:
        """
        Fulfill, queue, select and covert commands to be sent
        """
        commands = filter_commands(commands, self._filter_local_get)
        commands = self._command_queue.push_through(commands)
        return [self._write_next(cmd) for cmd in commands]

def store_literals(store: CacheStack, tasks: list[gtt.TaskNode]):
    """
    Walk the graph and commit all the literal tasks encountered to the store
    """
    oset = {t.name: t for t in tasks}
    cset: set[gtt.TaskName] = set()

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
                assert isinstance(dep_node, gtt.TaskNode)
                oset[dep_name] = dep_node

        # Store the embedded object if literal task
        if isinstance(task_node, gtt.LiteralTaskNode):
            store.put_serial(name, task_node.serialized)
