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
import resource

from typing import Iterable, TypeAlias
from dataclasses import dataclass
from contextlib import ExitStack

import psutil
import zmq

import galp.cli
import galp.net.core.types as gm
import galp.net.requests.types as gr
import galp.commands as cm
import galp.task_types as gtt
import galp.default_resources
from galp.result import Result, Ok, Error

from galp.store import StoreReadError, Store
from galp.protocol import make_stack, TransportMessage
from galp.zmq_async_transport import ZmqAsyncTransport
from galp.query import collect_task_inputs
from galp.task_types import TaskRef, load_step_by_key, NoSuchStep
from galp.net_store import handle_get, handle_stat, handle_upload
from galp.net.core.dump import add_request_id, Writer, get_request_id
from galp.asyn import filter_commands
from galp.context import set_path
from galp.logserver import logserver_connect

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

def make_worker(config: dict) -> 'Worker':
    """Prepare a worker factory function. Must be called in main thread.

    Args:
        args: an object whose attributes are the the parsed arguments, as
            returned by argparse.ArgumentParser.parse_args.
    """
    # Early setup, make sure this work before attempting config
    galp.cli.setup("worker", config.get('log_level'))
    galp.cli.set_sync_handlers()

    limit_resources(config.get('vm'), config.get('cpus_per_task'))
    if 'pin_cpus' in config:
        psutil.Process().cpu_affinity(config['pin_cpus'])

    # Validate config
    for key in ('store', 'endpoint'):
        if not key in config:
            raise TypeError(f'Worker config missing mandatory "{key}"')

    # Open store
    store_path = config.pop('store')
    logging.debug("Storing in %s", store_path)
    store = Store(store_path, gtt.TaskSerializer)


    logging.info("Worker starting, connecting to %s", config['endpoint'])

    return Worker(store, config)

SubReplyWriter: TypeAlias = Writer[Ok[gtt.FlatResultRef] | gr.RemoteError]

@dataclass
class Job:
    """
    A job to execute
    """
    write_reply: SubReplyWriter
    submit: gm.Submit
    inputs: Result[tuple[list, dict]]

class Worker:
    """
    Class representing an an async worker, wrapping transport, protocol and task
    execution logic.
    """
    def __init__(self, store: Store, setup: dict):
        def on_message(write: Writer[gm.Message], msg: gm.Message
                ) -> Iterable[TransportMessage] | Error:
            match msg:
                case gm.Get():
                    return handle_get(write, msg, store)
                case gm.Stat():
                    return handle_stat(write, msg, store)
                case gm.Upload():
                    return handle_upload(write, msg, store)
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
        self.mission = setup.get('mission', b'')
        self.store = store
        self.script = cm.Script()
        self.sock_logclient = setup.get('sock_logclient')

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

    def _get_serial(self, name: gtt.TaskName) -> Result[gtt.Serialized]:
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


    def _filter_local(self, command: cm.Primitive
                    ) -> tuple[list[TransportMessage], list[cm.Primitive]]:
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
                job = command.value
                del self.pending_jobs[job.submit.task_def.name]
                return [job.write_reply(
                    run_submission(self.store, self.sock_logclient, job)
                    )], []
            case _:
                raise NotImplementedError(command)

    def _new_commands_to_replies(self, commands: list[cm.Primitive]
            ) -> list[TransportMessage]:
        """
        Generate galp messages from a command reply list
        """
        return filter_commands(commands, self._filter_local)

    def schedule_task(self, write_reply: SubReplyWriter, msg: gm.Submit
                      ) -> list[cm.Primitive]:
        """
        Callback to schedule a task for execution.
        """
        task_def = msg.task_def

        # Ok to create a TaskDef as Submit implies deps have been run
        collection = collect_task_inputs(
            TaskRef(task_def.name), task_def
            )
        end: cm.Command[Job] = collection.eventually(lambda inputs: cm.End(
            Job(write_reply, msg, inputs)
            ))
        self.pending_jobs[task_def.name] = end
        return self.script.init_command(end)

    async def listen(self) -> Result[object]:
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

def run_submission(store: Store, sock_logclient, job: Job
                   ) -> Ok[gtt.ResultRef] | gr.RemoteError:
    """
    Actually run the task
    """
    task_def = job.submit.task_def
    inputs = job.inputs
    name = task_def.name
    step_name = task_def.step

    try:
        if isinstance(inputs, Error):
            logging.error('Could not gather task inputs' +
                          ' for step %s (%s): %s', name, step_name,
                          inputs.error)
            raise NonFatalTaskError(inputs.error)
        args, kwargs = inputs.value


        try:
            step = load_step_by_key(step_name)
        except NoSuchStep as exc:
            logging.exception('No such step known to worker: %s', step_name)
            raise NonFatalTaskError from exc

        # Put the task definition in store as soon as we've validated the
        # step and inputs. This allows to start interactive debugging from
        # just the task name is execution fails.
        store.put_task_def(task_def)

        logging.info('Executing step %s (%s)', name, step_name)
        try:
            # Set all the global changes:
            with ExitStack() as stack:
                # Set name of running task to use in generating unique files
                stack.enter_context(
                        set_path(store.dirpath, name.hex())
                        )
                # Propagate task resources to all child steps
                stack.enter_context(
                        galp.default_resources.resources(task_def.resources)
                        )
                # Set up redirect of std file descriptors
                stack.enter_context(
                        logserver_connect(get_request_id(job.submit),
                                          sock_logclient)
                        )

                # Actually run
                result = step.function(*args, **kwargs)
        except Exception as exc:
            logging.exception('Submitted task step failed: %s [%s]',
            step_name, name)
            raise NonFatalTaskError from exc

        # Store the result back
        result_ref = store.put_native(name, result, task_def.scatter)

        return Ok(result_ref)

    except NonFatalTaskError:
        # All raises include exception logging so it's safe to discard the
        # exception here
        return gr.RemoteError(
            f'Failed to execute task {step_name} [{name.hex()}], check worker logs'
            )
    except Exception as exc:
        # Ensures we log as soon as the error happens. The exception may be
        # re-logged afterwards.
        logging.exception('An unhandled error has occured within a step-' +
            'running asynchronous task. This signals a bug in GALP itself.')
        raise

def fork(config: dict) -> int:
    """
    Forks a worker with the given arguments.

    No validation is done on the arguments

    Args:
        config: configuration dictionnary for the started worker
    """
    return galp.cli.run_in_fork(main, config)

def main(config: dict):
    """
    Normal entry point
    """
    # Set cpu pins early, before we load any new module that may read it for its
    # init

    ret =  asyncio.run(make_worker(config).listen())
    obj = ret.unwrap()
    logging.info("Worker terminating normally")
    return obj

def add_parser_arguments(parser):
    """Add worker-specific arguments to the given parser"""
    parser.add_argument(
        'endpoint',
        help="Endpoint to bind to, in ZMQ format, e.g. tcp://127.0.0.2:12345 "
            "or ipc://path/to/socket ; see also man zmq_bind."
        )
    galp.store.add_store_argument(parser, optional=True)

    parser.add_argument('--vm',
        help='Limit on process virtual memory size, e.g. "2M" or "1G"')

    galp.cli.add_parser_arguments(parser)
