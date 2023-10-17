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

from typing import Awaitable
from dataclasses import dataclass

import psutil
import zmq
import zmq.asyncio

import galp.steps
import galp.cli
import galp.messages as gm
import galp.commands as cm
import galp.task_types as gtt

from galp.config import load_config
from galp.cache import StoreReadError, CacheStack
from galp.protocol import (ProtocolEndException,
    RoutedMessage, Replies)
from galp.reply_protocol import ReplyProtocol
from galp.zmq_async_transport import ZmqAsyncTransport
from galp.query import query
from galp.graph import NoSuchStep, Step
from galp.task_types import TaskRef, CoreTaskDef
from galp.profiler import Profiler

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

class WorkerProtocol(ReplyProtocol):
    """
    Handler for messages from the broker
    """
    def __init__(self, worker: 'Worker', store: CacheStack, name: str, router: bool):
        super().__init__(name, router)
        self.worker = worker
        self.store = store
        self.script = cm.Script(store=self.store)

    def route_message(self, orig: RoutedMessage | None,
                      new: gm.Message | RoutedMessage):
        """
        Always reply back to original message, if any.

        While the client may send unadressed messages to the broker for them to
        be allocated, the worker always replies to a request from an established
        client and never sends default-addressed messages.

        Note that the brokers fills in the worker address, so that messages that
        were originally sent to the broker are undistinguishable from messages
        directly addressed to the worker, from the worker's perspective.

        This makes the broker essentially transparent as far as the worker is
        concerned.
        """
        if isinstance(new, RoutedMessage):
            return new
        if orig is None:
            return super().route_message(None, new)
        return orig.reply(new)

    def on_illegal(self, msg: gm.Illegal):
        """
        Terminate
        """
        logging.error('Received ILLEGAL, terminating: %s', msg.reason)
        raise ProtocolEndException('Incoming ILLEGAL')

    def on_exit(self, msg: gm.Exit):
        """
        Terminate
        """
        del msg
        logging.info('Received EXIT, terminating')
        raise ProtocolEndException('Incoming EXIT')

    def on_get(self, msg: gm.Get):
        """
        Looks up the persistent store
        """
        name = msg.name
        logging.debug('Received GET for %s', name)
        reply: gm.Put | gm.NotFound
        try:
            data, children = self.store.get_serial(name)
            reply = gm.Put(name=name, data=data, children=children)
            logging.info('GET: Cache HIT: %s', name)
            return reply
        except KeyError:
            logging.info('GET: Cache MISS: %s', name)
        except StoreReadError:
            logging.exception('GET: Cache ERROR: %s', name)
        return gm.NotFound(name=name)

    def on_routed_message(self, msg: RoutedMessage) -> Replies:
        """
        Expose full message to submit handler
        """
        if isinstance(msg.body, gm.Exec):
            return self.on_routed_exec(msg, msg.body)
        return super().on_routed_message(msg)

    def on_routed_exec(self, request: RoutedMessage, msg: gm.Exec) -> Replies:
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

        # Set the resource limits immediately
        limit_task_resources(msg.resources)

        # Store hook. For now we just check the local cache, later we'll have a
        # central locking mechanism
        # NOTE: that's probably not correct for multi-output tasks !
        if self.store.contains(name):
            try:
                result_ref = self.store.get_children(name)
                logging.info('SUBMIT: Cache HIT: %s', name)
                return gm.Done(task_def=task_def, result=result_ref)
            except StoreReadError:
                logging.exception('SUBMIT: Failed cache hit: %s', name)
                return gm.Failed(task_def=task_def)

        # If not in cache, resolve metadata and run the task
        replies : list[gm.Message] = [gm.Doing(name=name)]

        # Process the list of GETs. This checks if they're in store,
        # and recursively finds new missing sub-resources when they are
        replies.extend(self.new_commands_to_replies(
            # Schedule the task first. It won't actually start until its inputs are
            # marked as available, and will return the list of GETs that are needed
            self.worker.schedule_task(request, sub)
            ))
        return replies

    def on_put(self, msg: gm.Put):
        """
        Put object in store, and mark the command as done
        """
        self.store.put_serial(msg.name, (msg.data, msg.children))
        return self.new_commands_to_replies(
            self.script.commands['GET', msg.name].done(msg.children)
            )

    def on_not_found(self, msg: gm.NotFound):
        """
        Propagate not found
        """
        reps = self.script.commands['GET', msg.name].failed('NOTFOUND')
        assert not reps

    def on_stat(self, msg: gm.Stat):
        """
        Wrapper around unsafe handler
        """
        try:
            return self._on_stat_io_unsafe(msg)
        except StoreReadError:
            logging.exception('STAT: ERROR %s', msg.name)
        return gm.NotFound(name=msg.name)

    def _on_stat_io_unsafe(self, msg: gm.Stat):
        """
        STAT handler allowed to raise store read errors
        """
        # Try first to extract both definition and children
        task_def = None
        try:
            task_def = self.store.get_task_def(msg.name)
        except KeyError:
            pass

        result_ref = None
        try:
            result_ref = self.store.get_children(msg.name)
        except KeyError:
            pass

        # Case 1: both def and children, DONE
        if task_def is not None and result_ref is not None:
            logging.info('STAT: DONE %s', msg.name)
            return gm.Done(task_def=task_def, result=result_ref)

        # Case 2: only def, FOUND
        if task_def is not None:
            logging.info('STAT: FOUND %s', msg.name)
            return gm.Found(task_def=task_def)

        # Case 3: only children
        # This means a legacy store that was missing tasks definition
        # persistency, or a corruption. This was originally treated as DONE, but
        # in the current model there is no way to make the peer accept a missing
        # or fake definition
        # if children is not None:

        # Case 4: nothing
        logging.info('STAT: NOT FOUND %s', msg.name)
        return gm.NotFound(name=msg.name)

    def new_commands_to_replies(self, commands: list[cm.InertCommand]) -> list[gm.Message]:
        """
        Generate galp messages from a command reply list
        """
        messages: list[gm.Message] = []

        while commands:
            command = commands.pop()
            if not isinstance(command, cm.Get):
                raise NotImplementedError(command)
            name = command.name
            try:
                result_ref = self.store.get_children(name)
                commands.extend(
                    self.script.commands[command.key].done(result_ref.children)
                    )
                continue
            except StoreReadError:
                logging.exception('In %s %s:', *command.key)
                commands.extend(
                    self.script.commands[command.key].failed('StoreReadError')
                    )
                continue
            except KeyError:
                pass
            messages.append(
                gm.Get(name=name)
                )
        return messages

@dataclass
class JobResult:
    """
    Result of executing a step

    Attributes:
        result: None iff the task has failed, other wise a reference to the
            stored result.
    """
    request: RoutedMessage
    submit: gm.Submit
    result: gtt.FlatResultRef | None

class Worker:
    """
    Class representing an an async worker, wrapping transport, protocol and task
    execution logic.
    """
    def __init__(self, setup: dict):
        self.protocol = WorkerProtocol(
            self, setup['store'],
            'BK', router=False)
        self.endpoint = setup['endpoint']
        self.transport = ZmqAsyncTransport(
            self.protocol,
            self.endpoint, zmq.DEALER # pylint: disable=no-member
            )
        self.step_dir = setup['steps']
        self.profiler = Profiler(setup.get('profile'))
        self.mission = setup.get('mission', b'')

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
        Loop that waits for tasks to finish to send back done/failed messages
        """
        reply: gm.Failed | gm.Done
        while True:
            task = await self.galp_jobs.get()
            job = await task
            task_def = job.submit.task_def
            if job.result is not None:
                reply = gm.Done(task_def=task_def, result=job.result)
            else:
                reply = gm.Failed(task_def=task_def)
            await self.transport.send_message(
                    self.protocol.route_message(job.request, reply)
                    )

    def schedule_task(self, request: RoutedMessage, msg: gm.Submit) -> list[cm.InertCommand]:
        """
        Callback to schedule a task for execution.
        """
        task_def = msg.task_def
        def _start_task(inputs: cm.FinalResult[list, str]):
            task = asyncio.create_task(
                self.run_submission(request, msg, inputs)
                )
            self.galp_jobs.put_nowait(task)

        script = self.protocol.script
        # References are safe by contract: a submit should only ever be sent
        # after its inputs are done
        collect = script.collect([
                query(script, TaskRef(tin.name), tin.op)
                for tin in [
                    *task_def.args,
                    *task_def.kwargs.values()
                    ]
                ])
        _, primitives = script.callback(collect, _start_task)
        return primitives

    async def listen(self):
        """
        Main message processing loop of the worker.
        """
        ready = gm.Ready(
            local_id=str(os.getpid()),
            mission=self.mission,
            )
        await self.transport.send_message(
                self.protocol.route_message(None, ready)
                )

        await self.transport.listen_reply_loop()

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

    async def run_submission(self, request: RoutedMessage, msg: gm.Submit,
                             inputs: cm.FinalResult[list, str]) -> JobResult:
        """
        Actually run the task
        """
        task_def = msg.task_def
        name = task_def.name
        step_name = task_def.step

        try:
            if isinstance(inputs, cm.Failed):
                logging.error('Could not gather task inputs'
                              ' for step %s (%s): %s', name, step_name,
                              inputs.error)
                raise NonFatalTaskError(inputs.error)

            logging.info('Executing step %s (%s)', name, step_name)

            try:
                step = self.step_dir.get(step_name)
            except NoSuchStep as exc:
                logging.exception('No such step known to worker: %s', step_name)
                raise NonFatalTaskError from exc

            args, kwargs = self.prepare_submit_arguments(step, task_def, inputs.result)

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

            return JobResult(request, msg, result_ref)

        except NonFatalTaskError:
            # All raises include exception logging so it's safe to discard the
            # exception here
            return JobResult(request, msg, None)
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
