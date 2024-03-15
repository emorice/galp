"""
Client api.

In contrast with a worker which is written as a standalone process, a client is just an
object that can be created and used as part of a larger program.
"""

import asyncio
import logging

from collections import defaultdict
from typing import Callable, Iterable, Any

import zmq
import zmq.asyncio

import galp.net.core.types as gm
import galp.net.requests.types as gr
import galp.commands as cm
import galp.task_types as gtt

from galp.result import Ok, Error
from galp.cache import CacheStack
from galp.net_store import handle_get
from galp.req_rep import handle_reply
from galp.protocol import (ProtocolEndException, make_stack,
    TransportMessage, Writer)
from galp.zmq_async_transport import ZmqAsyncTransport
from galp.control_queue import ControlQueue
from galp.query import run_task
from galp.task_types import (TaskName, TaskNode, LiteralTaskDef,
    ensure_task_node, TaskSerializer)

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
        n_cpus: number of cpus *per task* to allocate
    """

    def __init__(self, endpoint: str, cpus_per_task: int | None = None):
        # Memory only native+serial cache
        self.store = CacheStack(
            dirpath=None,
            serializer=TaskSerializer)
        def on_message(write: Writer[gm.Message], msg: gm.Message
                ) -> Iterable[TransportMessage] | Error:
            match msg:
                case gm.Get():
                    return handle_get(write, msg, self.store)
                case gm.Reply():
                    news = handle_reply(msg, self.protocol.script)
                    return self.protocol.schedule_new(news)
                case gm.NextRequest():
                    command = self.command_queue.on_next_item()
                    if command is None:
                        return []
                    message = self.protocol.write_next(command)
                    return [message]
                case _:
                    return Error(f'Unexpected {msg}')
        self.stack = make_stack(on_message, name='BK')
        self.command_queue: ControlQueue[cm.InertCommand] = ControlQueue()
        self.protocol = BrokerProtocol(
                command_queue=self.command_queue,
                write_local=self.stack.write_local,
                cpus_per_task=cpus_per_task or 1,
                store=self.store
                )
        self.transport = ZmqAsyncTransport(
            stack=self.stack,
            # pylint: disable=no-member # False positive
            endpoint=endpoint, socket_type=zmq.DEALER
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

        task_nodes = list(map(ensure_task_node, tasks))

        # Populate the store
        store_literals(self.store, task_nodes)

        cmd_vals = await asyncio.wait_for(
            self.run_collection(task_nodes, return_exceptions=return_exceptions,
                dry_run=dry_run),
            timeout=timeout)

        results: list[Any] = []
        for val in cmd_vals:
            match val:
                case Ok():
                    results.append(val.value)
                case cm.Pending():
                    # This should only be found if keep_going is False and an
                    # other command fails, so we should find a Failed and raise
                    # without actually returning this one.
                    # Fill it in anyway, to avoid confusions
                    results.append(val)
                case Error():
                    exc = TaskFailedError(
                        f'Failed to collect task: {val.error}'
                        )
                    if return_exceptions:
                        results.append(exc)
                    else:
                        raise exc

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

    async def run_collection(self, tasks: list[TaskNode],
            return_exceptions: bool, dry_run: bool
            ) -> list[cm.Result]:
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

        commands = [run_task(t, dry=dry_run) for t in tasks]
        collect = script.collect(commands)

        try:
            _, cmds = script.callback(collect, _end)
            await self.transport.send_messages(
                    self.protocol.schedule_new(cmds)
                    )
            err = await self.transport.listen_reply_loop()
            if err is not None:
                logging.error('Communication error: %s', err)
        except ProtocolEndException:
            pass
        # Issue 84: this work because End is only raised after collect is done,
        # but that's bad style.
        return [c.val for c in commands]

def store_literals(store: CacheStack, tasks: list[TaskNode]):
    """
    Walk the graph and commit all the literal tasks encountered to the store
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
            store.put_native(name, task_node.data)

class BrokerProtocol:
    """
    Main logic of the interaction of a client with a broker
    """
    def __init__(self, command_queue: ControlQueue[cm.InertCommand],
            write_local: Callable[[gm.Message], TransportMessage],
            cpus_per_task: int, store: CacheStack):
        self.write_local = write_local
        self.command_queue = command_queue
        self.store = store
        # Commands
        self.script = cm.Script()

        # Public attribute: counter for the number of SUBMITs for each task
        # Used for reporting and testing cache behavior
        self.submitted_count : defaultdict[TaskName, int] = defaultdict(int)

        # Default resources
        self.resources = gtt.ResourceClaim(cpus=cpus_per_task)

    def write_next(self, command: cm.InertCommand) -> TransportMessage:
        """
        Returns the next nessage to be sent for a task given the information we
        have about it.
        """
        assert command.is_pending()

        match command:
            case cm.Submit():
                name = command.task_def.name
                self.submitted_count[name] += 1
                sub = gm.Submit(task_def=command.task_def,
                                resources=self.get_resources(command.task_def))
                return self.write_local(sub)
            case cm.Get():
                return self.write_local(gm.Get(name=command.name))
            case cm.Stat():
                return self.write_local(gm.Stat(name=command.name))
            case _:
                raise NotImplementedError(command)

    def get_resources(self, task_def: gtt.CoreTaskDef) -> gtt.ResourceClaim:
        """
        Decide how much resources to allocate to a task
        """
        _ = task_def # to be used later
        return self.resources

    def filter_local_get(self, command: cm.InertCommand
                         ) -> tuple[list[cm.InertCommand], list[cm.InertCommand]]:
        """
        Fulfill GETs that are locally available
        """
        # Not a Get, leave as-is
        if not isinstance(command, cm.Get):
            return [command], []

        name = command.name
        try:
            res = gr.Put(*self.store.get_serial(name))
        except KeyError:
            # Not found, leave as-is
            return [command], []

        # Found, mark command as done and pass on children
        return [], self.script.commands['GET', name].done(Ok(res))

    def schedule_new(self, commands: Iterable[cm.InertCommand]
            ) -> list[TransportMessage]:
        """
        Fulfill, queue, select and covert commands to be sent
        """
        commands = cm.filter_commands(commands, self.filter_local_get)
        commands = self.command_queue.push_through(commands)
        return [self.write_next(cmd) for cmd in commands]
