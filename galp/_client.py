"""
Client api.

In contrast with a worker which is written as a standalone process, a client is just an
object that can be created and used as part of a larger program.
"""

import logging
import asyncio
import time

from collections import defaultdict
from typing import Callable, Iterable, Any

import zmq
import zmq.asyncio

import galp.messages as gm
import galp.commands as cm
import galp.task_types as gtt

from galp import async_utils
from galp.cache import CacheStack
from galp.net_store import make_get_handler
from galp.req_rep import make_reply_handler
from galp.protocol import (ProtocolEndException, make_stack, NameDispatcher,
        ChainDispatcher, make_type_dispatcher, make_name_dispatcher, Handler)
from galp.zmq_async_transport import ZmqAsyncTransport
from galp.command_queue import CommandQueue
from galp.query import run_task
from galp.task_types import (TaskName, TaskNode, LiteralTaskDef,
    ensure_task_node, TaskSerializer, SerializedTask)

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
        store = CacheStack(
            dirpath=None,
            serializer=TaskSerializer)
        self.protocol = BrokerProtocol(
                schedule=self.schedule, # callback to add tasks to the scheduling queue
                cpus_per_task=cpus_per_task or 1,
                store=store
                )
        stack = make_stack(
                lambda name, router : ChainDispatcher(
                    NameDispatcher(self.protocol), # Dummy
                    make_type_dispatcher([
                        make_illegal_hanlder(), # Illegal
                        make_get_handler(store), # Get
                        make_reply_handler(
                            # Found/NotFound/Done/Failed/Doing
                            make_name_dispatcher(self.protocol)
                            )
                        ])
                    ),
                name='BK',
                router=False
                )

        self.transport = ZmqAsyncTransport(
            stack=stack,
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

        cmd_vals = await asyncio.wait_for(
            self.run_collection(task_nodes, return_exceptions=return_exceptions,
                dry_run=dry_run),
            timeout=timeout)

        results: list[Any] = []
        for val in cmd_vals:
            match val:
                case cm.Done():
                    results.append(val.result)
                case cm.Pending():
                    # This should only be found if keep_going is False and an
                    # other command fails, so we should find a Failed and raise
                    # without actually returning this one.
                    # Fill it in anyway, to avoid confusions
                    results.append(val)
                case cm.Failed():
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
            self.protocol.schedule_new(cmds)

            await async_utils.run(
                self.process_scheduled(),
                self.transport.listen_reply_loop()
                )
        except ProtocolEndException:
            pass
        # Issue 84: this work because End is only raised after collect is done,
        # but that's bad style.
        return [c.val for c in commands]


def make_illegal_hanlder():
    """
    Raise when peer replies with Illegal

    Mostly useful for debugging
    """
    def on_illegal(_session, msg: gm.Illegal):
        """Should never happen"""
        raise RuntimeError(f'ILLEGAL recevived: {msg.reason}')
    return Handler(gm.Illegal, on_illegal)

class BrokerProtocol:
    """
    Main logic of the interaction of a client with a broker
    """
    def __init__(self, schedule: Callable[[cm.InertCommand], None],
            cpus_per_task: int, store: CacheStack):
        self.schedule = schedule
        self.store = store
        # Commands
        self.script = cm.Script()

        # Should be phased out, currently used to prevent re-sending a submit
        # after receiving a DOING notification
        self._started : defaultdict[TaskName, bool] = defaultdict(bool)

        # Public attributes: counters for the number of SUBMITs sent and DOING
        # received for each task
        # Used for reporting and testing cache behavior
        self.submitted_count : defaultdict[TaskName, int] = defaultdict(int)
        self.run_count : defaultdict[TaskName, int] = defaultdict(int)

        # Default resources
        self.resources = gtt.ResourceClaim(cpus=cpus_per_task)

        # Serializer for Put handler, to be modularized away
        self.serializer: TaskSerializer = TaskSerializer()

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

    def write_next(self, command: cm.InertCommand) -> gm.Message | None:
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
                if self._started[name]:
                    # Early stop if we received DOING
                    return None
                self.submitted_count[name] += 1
                sub = gm.Submit(task_def=command.task_def,
                                resources=self.get_resources(command.task_def))
                return sub
            case cm.Get():
                get = self.get(command.name)
                if get is not None:
                    return get
            case cm.Stat():
                return gm.Stat(name=command.name)
            case _:
                raise NotImplementedError(command)

        return None

    def get_resources(self, task_def: gtt.CoreTaskDef) -> gtt.ResourceClaim:
        """
        Decide how much resources to allocate to a task
        """
        _ = task_def # to be used later
        return self.resources

    # Custom protocol sender
    # ======================

    def get(self, name: TaskName) -> gm.Get | None:
        """
        Send GET for task if not locally available
        """
        try:
            data, children = self.store.get_serial(name)
        except KeyError:
            # Not found, send a normal GET
            return gm.Get(name=name)

        # Found, mark command as done and pass on children
        self.schedule_new(
            self.script.commands['GET', name].done(
                SerializedTask(self.serializer, data, children)
                )
            )
        # Supress normal output, removing task from queue
        return None

    # Protocol callbacks
    # ==================
    def on_put(self, msg: gm.Put):
        """
        Fulfill promise.

        If the object has parts (children), the recursive rget promise is
        responsible for issuing the corresponsing new requests.
        """
        # Inject serializer
        serialized = SerializedTask(self.serializer, msg.data, msg.children)
        # Mark as done and sets result
        self.schedule_new(
            self.script.commands['GET', msg.name].done(serialized)
            )

    def on_done(self, msg: gm.Done) -> None:
        """Given that done_task just finished, mark it as done, letting the
        command system schedule any dependent ready to be submitted, schedule
        GETs for final tasks, etc.
        """
        task_def = msg.task_def
        name = task_def.name

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
        self._started[msg.name] = True

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
    def schedule_new(self, commands: Iterable[cm.InertCommand]) -> None:
        """
        Transfer the command queue to the scheduler
        """
        for command in commands:
            self.schedule(command)
