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

from typing import Iterable, TypeAlias
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
from galp.cache import StoreReadError
from galp.protocol import ProtocolEndException, make_stack, TransportMessage
from galp.zmq_async_transport import ZmqAsyncTransport
from galp.query import query
from galp.graph import NoSuchStep, Step
from galp.task_types import TaskRef, CoreTaskDef
from galp.profiler import Profiler
from galp.net_store import handle_get, handle_stat, handle_upload
from galp.net.core.dump import add_request_id, Writer
from galp.asyn import filter_commands

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

SubReplyWriter: TypeAlias = Writer[Result[gtt.FlatResultRef, gr.RemoteError]]

@dataclass
class Job:
    """
    A job to execute
    """
    write_reply: SubReplyWriter
    submit: gm.Submit
    inputs: Result[list[object], Error]

class Worker:
    """
    Class representing an an async worker, wrapping transport, protocol and task
    execution logic.
    """
    def __init__(self, setup: dict):
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
                case gm.Upload():
                    return handle_upload(write, msg, setup['store'])
                case gm.Exec():
                    return self.on_routed_exec(write, msg)
                case gm.Reply():
                    return self._new_commands_to_replies(
                            self.script.done(msg.request, msg.value)
                            )
                case _:
                    return Error(f'Unexpected {msg}')

        self.transport = ZmqAsyncTransport(
                make_stack(on_message, name='BK'),
                setup['endpoint'], zmq.DEALER # pylint: disable=no-member
                )
        self.step_dir = setup['steps']
        self.profiler = Profiler(setup.get('profile'))
        self.mission = setup.get('mission', b'')
        self.store = setup['store']
        self.script = cm.Script()

        # Submitted jobs
        self.pending_jobs: dict[gtt.TaskName, cm.Command] = {}

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
        return self._new_commands_to_replies(
            # Schedule the task first. It won't actually start until its inputs are
            # marked as available, and will return the list of GETs that are needed
            self.schedule_task(write_reply, sub)
            )

    def _get_serial(self, name: gtt.TaskName) -> Result[gtt.Serialized, Error]:
        """
        Helper to safely attempt to read from store
        """
        try:
            return Ok(self.store.get_serial(name))
        except StoreReadError:
            logging.exception('In %s:', name)
            return Error('StoreReadError')
        except KeyError:
            return Error('Not found in store')


    def _filter_local(self, command: cm.InertCommand
                    ) -> tuple[list[TransportMessage], list[cm.InertCommand]]:
        """Filter out locally available resources, send queries for the rest"""
        match command:
            case cm.Send():
                match (req := command.request):
                    case gm.Get():
                        return [], self.script.done(
                                command.key,
                                self._get_serial(req.name)
                                )
                    case _:
                        raise NotImplementedError(command)
            case cm.End():
                return [self.run_submission(command.value)], []
            case _:
                raise NotImplementedError(command)

    def _new_commands_to_replies(self, commands: list[cm.InertCommand]
            ) -> list[TransportMessage]:
        """
        Generate galp messages from a command reply list
        """
        return filter_commands(commands, self._filter_local)

    def schedule_task(self, write_reply: SubReplyWriter, msg: gm.Submit
                      ) -> list[cm.InertCommand]:
        """
        Callback to schedule a task for execution.
        """
        task_def = msg.task_def

        # References are safe by contract: a submit should only ever be sent
        # after its inputs are done
        collect = self.script.collect([
                query(TaskRef(tin.name), tin.op)
                for tin in [
                    *task_def.args,
                    *task_def.kwargs.values()
                    ]
                ], keep_going=False)
        end: cm.Command = collect.eventually(lambda inputs: cm.End(
            Job(write_reply, msg, inputs)
            ))
        self.pending_jobs[task_def.name] = end
        return self.script.init_command(end)

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
        Unflatten collected arguments and inject path maker if requested
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
                self.store.dirpath,
                task_def.name.hex()
                )
            kwargs['_galp'] = path_maker

        return args, kwargs

    def run_submission(self, job: Job) -> TransportMessage:
        """
        Actually run the task
        """
        task_def = job.submit.task_def
        inputs = job.inputs
        name = task_def.name
        del self.pending_jobs[name]
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
            self.store.put_task_def(task_def)
            result_ref = self.store.put_native(name, result, task_def.scatter)

            return job.write_reply(Ok(result_ref))

        except NonFatalTaskError:
            # All raises include exception logging so it's safe to discard the
            # exception here
            return job.write_reply(gr.RemoteError(
                f'Failed to execute task {task_def}, check worker logs'
                ))
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
        ret = await worker.listen()
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
