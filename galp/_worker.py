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

from typing import Any, Awaitable
from dataclasses import dataclass

import psutil
import zmq
import zmq.asyncio

import galp.steps
import galp.cli

from galp.config import load_config
from galp.cache import StoreReadError, CacheStack
from galp.lower_protocol import MessageList
from galp.protocol import ProtocolEndException, IllegalRequestError
from galp.reply_protocol import ReplyProtocol
from galp.zmq_async_transport import ZmqAsyncTransport
from galp.commands import Script
from galp.query import Query
from galp.profiler import Profiler
from galp.graph import NoSuchStep, Block
from galp.task_types import NamedCoreTaskDef, TaskName

class NonFatalTaskError(RuntimeError):
    """
    An error has occured when running a step, but the worker should keep
    running.
    """

def limit_resources(vm_limit=None):
    """
    Set resource limits from command line, for now only virtual memory.

    We use base-10 prefixes: when in doubt, it's safer to
    set a stricter limit.
    """
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

    # Parse configuration
    setup = load_config(config,
            mandatory=['endpoint', 'store']
            )

    # Late setup
    limit_resources(setup.get('vm'))

    os.makedirs(os.path.join(setup['store'].dirpath, 'galp'), exist_ok=True)

    logging.info("Worker connecting to %s", setup['endpoint'])

    def _make_worker():
        return Worker(
            setup['endpoint'], setup['store'],
            setup['steps'],
            Profiler(setup.get('profile'))
            )
    return _make_worker

class WorkerProtocol(ReplyProtocol):
    """
    Handler for messages from the broker
    """
    def __init__(self, worker: 'Worker', store: CacheStack, name: str, router: bool):
        super().__init__(name, router)
        self.worker = worker
        self.store = store
        self.script = Script(store=self.store)

    def on_invalid(self, route, reason):
        logging.error('Bad request: %s', reason)
        return super().on_invalid(route, reason)

    def on_illegal(self, route, reason):
        logging.error('Received ILLEGAL, terminating: %s', reason)
        raise ProtocolEndException('Incoming ILLEGAL')

    def on_exit(self, route):
        logging.info('Received EXIT, terminating')
        raise ProtocolEndException('Incoming EXIT')

    def on_get(self, route, name):
        logging.debug('Received GET for %s', name)
        try:
            serialized = self.store.get_serial(name)
            reply = self.put(route, name, serialized)
            logging.info('GET: Cache HIT: %s', name)
            return reply
        except KeyError:
            logging.info('GET: Cache MISS: %s', name)
        except StoreReadError:
            logging.exception('GET: Cache ERROR: %s', name)
        return self.not_found(route, name)

    def on_submit(self, route, named_def: NamedCoreTaskDef):
        """Start processing the submission asynchronously.

        This means returning immediately to the event loop, which allows
        processing further messages needed for the task execution (resource
        exchange, sub-tasks, ...).

        This the only asynchronous handler, all others are semantically blocking.
        """
        tdef = named_def.task_def
        logging.info('SUBMIT: %s', tdef.step)
        name = named_def.name

        # Store hook. For now we just check the local cache, later we'll have a
        # central locking mechanism
        # NOTE: that's probably not correct for multi-output tasks !
        if self.store.contains(name):
            logging.info('SUBMIT: Cache HIT: %s', name)
            return self.done(route, named_def, [])

        # If not in cache, resolve metadata and run the task
        replies = MessageList([self.doing(route, name)])

        # Schedule the task first. It won't actually start until its inputs are
        # marked as available, and will return the list of GETs that are needed
        self.worker.schedule_task(route, named_def)

        # Process the list of GETs. This checks if they're in store,
        # and recursively finds new missing sub-resources when they are
        more_replies = self.new_commands_to_messages(route)

        return replies + more_replies

    def on_put(self, route, name: TaskName, serialized: tuple[bytes, list[TaskName]]):
        """
        Put object in store, and mark the command as done
        """
        self.store.put_serial(name, serialized)
        _data, children = serialized
        self.script.commands['GET', name].done(children)
        return self.new_commands_to_messages(route)

    def on_not_found(self, route, name):
        self.script.commands['GET', name].failed('NOTFOUND')
        assert not self.script.new_commands

    def on_stat(self, route, name):
        try:
            return self._on_stat_io_unsafe(route, name)
        except StoreReadError:
            logging.exception('STAT: ERROR %s', name)
        return self.not_found(route, name)

    def _on_stat_io_unsafe(self, route, name: TaskName):
        """
        STAT handler allowed to raise store read errors
        """
        # Try first to extract both definition and children
        named_def = None
        try:
            named_def = self.store.get_task_def(name)
        except KeyError:
            pass

        children = None
        try:
            children = self.store.get_children(name)
        except KeyError:
            pass

        # Case 1: both def and children, DONE
        if named_def is not None and children is not None:
            logging.info('STAT: DONE %s', name)
            return self.done(route, named_def, children)

        # Case 2: only def, FOUND
        if named_def is not None:
            logging.info('STAT: FOUND %s', name)
            return self.found(route, named_def)

        # Case 3: only children
        # This means a legacy store that was missing tasks definition
        # persistency, or a corruption. This was originally treated as DONE, but
        # in the current model there is no way to make the peer accept a missing
        # or fake definition
        # if children is not None:

        # Case 4: nothing
        logging.info('STAT: NOT FOUND %s', name)
        return self.not_found(route, name)

    def new_commands_to_messages(self, route):
        """
        Generate galp messages from a command reply list
        """
        messages = MessageList()
        while self.script.new_commands:
            verb, name = self.script.new_commands.popleft()
            if verb != 'GET':
                if verb in ('STAT', 'SUBMIT'):
                    # Avoid hanging if a higher layer mistakenly try to use a
                    # client-side command
                    raise NotImplementedError(verb)
                continue
            try:
                children = self.store.get_children(name)
                self.script.commands[verb, name].done(children)
                continue
            except StoreReadError:
                logging.exception('In %s %s:', verb, name)
                self.script.commands[verb, name].failed('StoreReadError')
                continue
            except KeyError:
                pass
            messages.append(
                self.get(route, name)
                )
        return messages

@dataclass
class JobResult:
    """
    Result of executing a step
    """
    route: Any
    named_def: NamedCoreTaskDef
    success: bool
    result: list[TaskName]

class Worker:
    """
    Class representing an an async worker, wrapping transport, protocol and task
    execution logic.
    """
    def __init__(self, endpoint: str, store: CacheStack, step_dir: Block,
            profiler: Profiler):
        self.protocol = WorkerProtocol(
            self, store,
            'BK', router=False)
        self.transport = ZmqAsyncTransport(
            self.protocol,
            endpoint, zmq.DEALER # pylint: disable=no-member
            )
        self.endpoint = endpoint
        self.step_dir = step_dir
        self.profiler = profiler

        # Submitted jobs
        self.galp_jobs : asyncio.Queue[Awaitable[JobResult]] = asyncio.Queue()

    def run(self):
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
        Loops that waits for tasks to finsish to send back done/failed messages
        """
        while True:
            task = await self.galp_jobs.get()
            job = await task
            if job.success:
                reply = self.protocol.done(job.route, job.named_def,  job.result)
            else:
                reply = self.protocol.failed(job.route, job.named_def)
            await self.transport.send_message(reply)

    def schedule_task(self, client_route, named_def: NamedCoreTaskDef):
        """
        Callback to schedule a task for execution.
        """
        def _start_task(status, inputs):
            task = asyncio.create_task(
                self.run_submission(status, client_route, named_def,
                    inputs)
                )
            self.galp_jobs.put_nowait(task)

        tdef = named_def.task_def
        script = self.protocol.script
        collect = script.collect([
                Query(script, tin.name, tin.op)
                for tin in [
                    *tdef.args,
                    *tdef.kwargs.values()
                    ]
                ])
        script.callback(collect, _start_task)

    async def listen(self):
        """
        Main message processing loop of the worker.
        """
        ready = self.protocol.ready(
            self.protocol.default_route(),
            str(os.getpid()).encode('ascii')
            )
        await self.transport.send_message(ready)

        await self.transport.listen_reply_loop()

    # Task execution logic
    # ====================

    async def run_submission(self, status, route, named_def: NamedCoreTaskDef,
            inputs) -> JobResult:
        """
        Actually run the task
        """
        name = named_def.name
        tdef = named_def.task_def
        step_name = tdef.step
        arg_tins = tdef.args
        kwarg_tins = tdef.kwargs

        try:
            if status:
                logging.error('Could not gather task inputs'
                        ' for step %s (%s)', name, step_name)
                raise NonFatalTaskError

            logging.info('Executing step %s (%s)', name, step_name)

            try:
                step = self.step_dir.get(step_name)
            except NoSuchStep as exc:
                logging.exception('No such step known to worker: %s', step_name)
                raise NonFatalTaskError from exc

            # Unpack inputs from flattened input list
            try:
                inputs = list(reversed(inputs))
                args = []
                for _tin in arg_tins:
                    args.append(inputs.pop())
                kwargs = {}
                for keyword in kwarg_tins:
                    kwargs[keyword] = inputs.pop()
            except UnicodeDecodeError as exc:
                # Either you tried to use python's non-ascii keywords feature,
                # or more likely you messed up the encoding on the caller side.
                raise IllegalRequestError(route,
                    f'Cannot decode keyword {exc.object!r}'
                    f' for step {name} ({step_name})'
                    ) from exc

            # Inject path maker if requested
            if '_galp' in step.kw_names:
                path_maker = PathMaker(
                    self.protocol.store.dirpath,
                    name.hex()
                    )
                kwargs['_galp'] = path_maker

            # This may block for a long time, by design
            try:
                result = self.profiler.wrap(name, step)(*args, **kwargs)
            except Exception as exc:
                logging.exception('Submitted task step failed: %s [%s]',
                step_name, name)
                raise NonFatalTaskError from exc

            # Store the result back, along with the task definition
            self.protocol.store.put_task_def(named_def)
            children = self.protocol.store.put_native(name, result, tdef.scatter)

            return JobResult(route, named_def, True, children)

        except NonFatalTaskError:
            # All raises include exception logging so it's safe to discard the
            # exception here
            return JobResult(route, named_def, False, [])
        except Exception as exc:
            # Ensures we log as soon as the error happens. The exception may be
            # re-logged afterwards.
            logging.exception('An unhandled error has occured within a step-'
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
    galp.cache.add_store_argument(parser)

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
