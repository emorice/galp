"""
Worker, e.g the smallest unit that takes a job request, perform it synchronously
and returns a pass/fail notification.

Note that everything more complicated (heartbeating, load balancing, pool
management, etc) is left to upstream proxys or nannys. The only forseeable change is that
we may want to hook a progress notification, through a function callable from
within the job and easy to patch if the code has to be run somewhere else.
"""

import os
import asyncio
import logging
import argparse
import resource

from typing import Awaitable, Iterable, TypeAlias
from dataclasses import dataclass

import psutil
import zmq
import zmq.asyncio

import galp.steps
import galp.cli
import galp.net.core.types as gm
import galp.net.requests.types as gr
import galp.commands as cm
import galp.task_types as gtt
from galp.result import Result, Ok, Error

from galp.config import load_config
from galp.cache import StoreReadError, CacheStack
from galp.protocol import ProtocolEndException, make_stack, TransportMessage
from galp.zmq_async_transport import ZmqAsyncTransport
from galp.query import query
from galp.graph import NoSuchStep, Step
from galp.task_types import TaskRef, CoreTaskDef
from galp.profiler import Profiler
from galp.net_store import handle_get, handle_stat
from galp.net.core.dump import add_request_id, Writer
from galp.req_rep import handle_reply

class NonFatalTaskError(RuntimeError):
    """
    An error has occured when running a step, but the worker should keep
    running.
    """

def limit_resources(vm_limit=None, cpus=None):
    """
    Set resource limits from command line, for now only virtual memory

    We use base-10 prefixes: when in doubt, it's safer to
    set a stricter limit.
    """
    # Note: this should be done per task, not globally at the beginning
    suffixes = {
        'K': 3,
        'M': 6,
        'G': 9
        }
    if vm_limit:
        opt_suffix = vm_limit[-1]
        exp = suffixes.get(opt_suffix)
        if exp is not None:
            mult = 10**exp
            size = int(vm_limit[:-1]) * mult
        else:
            size = int(vm_limit)

        logging.info('Setting virtual memory limit to %d bytes', size)

        _soft, hard = resource.getrlimit(resource.RLIMIT_AS)
        resource.setrlimit(resource.RLIMIT_AS, (size, hard))

    # This should not be necessary, because we pin the right number of cpus
    # before reaching here, and most libraries we care about inspect pins to set
    # the default pool size.
    # We keep it commented out as documentation of the former behavior.
    # Furthermore, we should need nothing at startup anyways, see issue #89
    # if cpus:
    #     import threadpoolctl # type: ignore[import]
    #     # pylint: disable=import-outside-toplevel
    #     import numpy # pylint: disable=unused-import # side effect
    #     threadpoolctl.threadpool_limits(cpus)
    del cpus

def limit_task_resources(resources: gtt.Resources):
    """
    Set resource limits from received request, for each task independently
    """
    psutil.Process().cpu_affinity(resources.cpus)

def make_worker_init(config):
    """Prepare a worker factory function. Must be called in main thread.

    This involves parsing the config and loading plug-in steps.

    Args:
        args: an object whose attributes are the the parsed arguments, as
            returned by argparse.ArgumentParser.parse_args.
    """
    if config.get('steps'):
        config['steps'].append('galp.steps')
    else:
        config['steps'] = ['galp.steps']

    # Early setup, make sure this work before attempting config
    galp.cli.setup("worker", config.get('log_level'))
    galp.cli.set_sync_handlers()
    limit_resources(config.get('vm'), config.get('cpus_per_task'))

    # Parse configuration
    setup = load_config(config,
            mandatory=['endpoint', 'store']
            )

    os.makedirs(os.path.join(setup['store'].dirpath, 'galp'), exist_ok=True)

    logging.info("Worker connecting to %s", setup['endpoint'])

    def _make_worker():
        return Worker(setup)
    return _make_worker

class WorkerProtocol:
    """
    Handler for messages from the broker
    """
    def __init__(self, worker: 'Worker', store: CacheStack):
        self.worker = worker
        self.store = store
        self.script = cm.Script()

    def on_routed_exec(self, write: Writer[gm.Message], msg: gm.Exec
            ) -> list[list[bytes]]:
        """Start processing the submission asynchronously.

        This means returning immediately to the event loop, which allows
        processing further messages needed for the task execution (resource
        exchange, sub-tasks, ...).

        This the only asynchronous handler, all others are semantically blocking.
        """
        sub = msg.submit
        task_def = sub.task_def
        logging.info('SUBMIT: %s on cpus %s', task_def.step, msg.resources.cpus)
        name = task_def.name
        # Not that we reply to the Submit, not the Exec
        write_reply = add_request_id(write, sub)

        # Set the resource limits immediately
        limit_task_resources(msg.resources)

        # Store hook. For now we just check the local cache, later we'll have a
        # central locking mechanism
        # NOTE: that's probably not correct for multi-output tasks !
        if self.store.contains(name):
            try:
                result_ref = self.store.get_children(name)
                logging.info('SUBMIT: Cache HIT: %s', name)
                return [write_reply(Ok(result_ref))]
            except StoreReadError:
                err = f'Failed cache hit: {name}'
                logging.exception(err)
                return [write_reply(gr.RemoteError(err))]

        # Process the list of GETs. This checks if they're in store,
        # and recursively finds new missing sub-resources when they are
        return self.new_commands_to_replies(write,
            # Schedule the task first. It won't actually start until its inputs are
            # marked as available, and will return the list of GETs that are needed
            self.worker.schedule_task(write_reply, sub)
            )

    def new_commands_to_replies(self, write: Writer[gm.Message], commands: list[cm.InertCommand]
            ) -> list[TransportMessage]:
        """
        Generate galp messages from a command reply list
        """

        def _filter_local(command: cm.InertCommand
                    ) -> tuple[list[TransportMessage], list[cm.InertCommand]]:
            """Filter out locally available resources, send queries for the rest"""
            if not isinstance(command, cm.Get):
                raise NotImplementedError(command)
            name = command.name
            try:
                res = gr.Put(*self.store.get_serial(name))
                return [], self.script.commands[command.key].done(Ok(res))
            except StoreReadError:
                logging.exception('In %s %s:', *command.key)
                return [], self.script.commands[command.key].done(Error('StoreReadError'))
            except KeyError:
                return [write(gm.Get(name=name))], []
        return cm.filter_commands(commands, _filter_local)

SubReplyWriter: TypeAlias = Writer[Result[gtt.FlatResultRef, gr.RemoteError]]

@dataclass
class JobResult:
    """
    Result of executing a step

    Attributes:
        session: communication handle to the client to use to send a reply back
        result: None iff the task has failed, other wise a reference to the
            stored result.
    """
    write_reply: SubReplyWriter
    submit: gm.Submit
    result: gtt.FlatResultRef | None

class Worker:
    """
    Class representing an an async worker, wrapping transport, protocol and task
    execution logic.
    """
    def __init__(self, setup: dict):
        protocol = WorkerProtocol(self, setup['store'])

        def on_message(write: Writer[gm.Message], msg: gm.Message
                ) -> Iterable[TransportMessage] | Error:
            match msg:
                case gm.Exit():
                    logging.info('Received EXIT, terminating')
                    raise ProtocolEndException('Incoming EXIT')
                case gm.Get():
                    return handle_get(write, msg, setup['store'])
                case gm.Stat():
                    return handle_stat(write, msg, setup['store'])
                case gm.Exec():
                    return protocol.on_routed_exec(write, msg)
                case gm.Reply():
                    news = handle_reply(msg, protocol.script)
                    return protocol.new_commands_to_replies(write, news)
                case _:
                    return Error(f'Unexpected {msg}')

        self.protocol : 'WorkerProtocol' = protocol
        self.transport = ZmqAsyncTransport(
                make_stack(on_message, name='BK'),
                setup['endpoint'], zmq.DEALER # pylint: disable=no-member
                )
        self.step_dir = setup['steps']
        self.profiler = Profiler(setup.get('profile'))
        self.mission = setup.get('mission', b'')

        # Submitted jobs
        self.galp_jobs : asyncio.Queue[Awaitable[JobResult]] = asyncio.Queue()

    def run(self) -> list[asyncio.Task]:
        """
        Starts and returns life-long tasks. You should cancel the others as soon
        as any finishes or raises
        """
        tasks = [
            asyncio.create_task(self.monitor_jobs()),
            asyncio.create_task(self.listen())
            ]
        return tasks

    async def monitor_jobs(self) -> None:
        """
        Loop that waits for tasks to finish to send back done/failed messages
        """
        reply: gr.RemoteError | Ok[gtt.FlatResultRef]
        while True:
            task = await self.galp_jobs.get()
            job = await task
            task_def = job.submit.task_def
            if job.result is not None:
                reply = Ok(job.result)
            else:
                reply = gr.RemoteError(
                        f'Failed to execute task {task_def}, check worker logs'
                        )
            await self.transport.send_raw(job.write_reply(reply))

    def schedule_task(self, write_reply: SubReplyWriter, msg: gm.Submit
                      ) -> list[cm.InertCommand]:
        """
        Callback to schedule a task for execution.
        """
        task_def = msg.task_def
        def _start_task(inputs: Result[list, Error]):
            task = asyncio.create_task(
                self.run_submission(write_reply, msg, inputs)
                )
            self.galp_jobs.put_nowait(task)

        script = self.protocol.script
        # References are safe by contract: a submit should only ever be sent
        # after its inputs are done
        collect = script.collect([
                query(TaskRef(tin.name), tin.op)
                for tin in [
                    *task_def.args,
                    *task_def.kwargs.values()
                    ]
                ])
        _, primitives = script.callback(collect, _start_task)
        return primitives

    async def listen(self) -> Error | None:
        """
        Main message processing loop of the worker.
        """
        ready = gm.Ready(
            local_id=str(os.getpid()),
            mission=self.mission,
            )
        await self.transport.send_message(ready)

        return await self.transport.listen_reply_loop()

    # Task execution logic
    # ====================

    def prepare_submit_arguments(self, step: Step, task_def: CoreTaskDef, inputs: list
                                     ) -> tuple[list, dict]:
        """
        Unflatten collected arguments and inject path makek if requested
        """
        r_inputs = list(reversed(inputs))
        args = []
        for _tin in task_def.args:
            args.append(r_inputs.pop())
        kwargs = {}
        for keyword in task_def.kwargs:
            kwargs[keyword] = r_inputs.pop()

        # Inject path maker if requested
        if '_galp' in step.kw_names:
            path_maker = PathMaker(
                self.protocol.store.dirpath,
                task_def.name.hex()
                )
            kwargs['_galp'] = path_maker

        return args, kwargs

    async def run_submission(self, write_reply: SubReplyWriter, msg: gm.Submit,
                             inputs: Result[list, Error]) -> JobResult:
        """
        Actually run the task
        """
        task_def = msg.task_def
        name = task_def.name
        step_name = task_def.step

        try:
            if isinstance(inputs, Error):
                logging.error('Could not gather task inputs' +
                              ' for step %s (%s): %s', name, step_name,
                              inputs.error)
                raise NonFatalTaskError(inputs.error)

            logging.info('Executing step %s (%s)', name, step_name)

            try:
                step = self.step_dir.get(step_name)
            except NoSuchStep as exc:
                logging.exception('No such step known to worker: %s', step_name)
                raise NonFatalTaskError from exc

            args, kwargs = self.prepare_submit_arguments(step, task_def, inputs.value)

            # This may block for a long time, by design
            try:
                result = self.profiler.wrap(name, step)(*args, **kwargs)
            except Exception as exc:
                logging.exception('Submitted task step failed: %s [%s]',
                step_name, name)
                raise NonFatalTaskError from exc

            # Store the result back, along with the task definition
            self.protocol.store.put_task_def(task_def)
            result_ref = self.protocol.store.put_native(name, result, task_def.scatter)

            return JobResult(write_reply, msg, result_ref)

        except NonFatalTaskError:
            # All raises include exception logging so it's safe to discard the
            # exception here
            return JobResult(write_reply, msg, None)
        except Exception as exc:
            # Ensures we log as soon as the error happens. The exception may be
            # re-logged afterwards.
            logging.exception('An unhandled error has occured within a step-' +
                'running asynchronous task. This signals a bug in GALP itself.')
            raise

@dataclass
class PathMaker:
    """
    State keeping path to generate new paths inside steps
    """
    dirpath: str
    task: str
    fileno: int = 0

    def new_path(self):
        """
        Returns a new unique path
        """
        fileno = self.fileno
        self.fileno += 1
        return os.path.join(
            self.dirpath,
            'galp',
            f'{self.task}_{fileno}'
            )

def fork(config):
    """
    Forks a worker with the given arguments.

    No validation is done on the arguments

    Args:
        config: configuration dictionnary for the started worker
    """
    return galp.cli.run_in_fork(main, config)

def main(config):
    """
    Normal entry point
    """
    # Set cpu pins early, before we load any new module that may read it for its
    # init
    if 'pin_cpus' in config:
        psutil.Process().cpu_affinity(config['pin_cpus'])

    make_worker = make_worker_init(config)
    async def _coro(make_worker):
        worker = make_worker()
        ret = await galp.cli.wait(worker.run())
        logging.info("Worker terminating normally")
        return ret
    return asyncio.run(_coro(make_worker))

def add_parser_arguments(parser):
    """Add worker-specific arguments to the given parser"""
    parser.add_argument(
        'endpoint',
        help="Endpoint to bind to, in ZMQ format, e.g. tcp://127.0.0.2:12345 "
            "or ipc://path/to/socket ; see also man zmq_bind."
        )
    galp.cache.add_store_argument(parser, optional=True)

    parser.add_argument('--vm',
        help='Limit on process virtual memory size, e.g. "2M" or "1G"')

    galp.cli.add_parser_arguments(parser)

def make_parser():
    """
    Creates argument parser and configures it
    """
    _parser = argparse.ArgumentParser()
    add_parser_arguments(_parser)
    return _parser
