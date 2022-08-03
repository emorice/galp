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
import importlib

import zmq
import zmq.asyncio
import toml

import galp.steps
import galp.cache
import galp.cli
from galp.config import ConfigError
from galp.lower_protocol import MessageList
from galp.protocol import ProtocolEndException, IllegalRequestError
from galp.reply_protocol import ReplyProtocol
from galp.zmq_async_transport import ZmqAsyncTransport
from galp.script import Script, AllDone
from galp.commands import define_commands

from galp.profiler import Profiler
from galp.serializer import Serializer
from galp.eventnamespace import NoHandlerError

class NonFatalTaskError(RuntimeError):
    """
    An error has occured when running a step, but the worker should keep
    running.
    """

def load_steps(plugin_names):
    """
    Attempts to import the given modules, and add their `export` attribute to
    the list of currently known steps
    """
    step_dir = galp.steps.export
    for name in plugin_names:
        try:
            plugin = importlib.import_module(name)
            step_dir += plugin.export
            logging.info('Loaded plug-in %s', name)
        except ModuleNotFoundError as exc:
            logging.error('No such plug-in: %s', name)
            raise ConfigError(('No such plugin', name)) from exc
        except AttributeError as exc:
            logging.error('Plug-in %s do not expose "export"', name)
            raise ConfigError(('Bad plugin', name)) from exc
    return step_dir

def limit_resources(args):
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
    if args.vm:
        opt_suffix = args.vm[-1]
        exp = suffixes.get(opt_suffix)
        if exp is not None:
            mult = 10**exp
            size = int(args.vm[:-1]) * mult
        else:
            size = int(args.vm)

        logging.info('Setting virtual memory limit to %d bytes', size)

        _soft, hard = resource.getrlimit(resource.RLIMIT_AS)
        resource.setrlimit(resource.RLIMIT_AS, (size, hard))

async def main(args):
    """Entry point for the worker program

    Args:
        args: an object whose attributes are the the parsed arguments, as
            returned by argparse.ArgumentParser.parse_args.
    """
    galp.cli.setup(args, "worker")
    # Signal handler
    galp.cli.set_sync_handlers()

    config = {}
    if args.config:
        logging.info("Loading config file %s", args.config)
        # TOML is utf-8 by spec
        with open(args.config, encoding='utf-8') as fstream:
            config = toml.load(fstream)

    limit_resources(args)

    step_dir = load_steps(config['steps'] if 'steps' in config else [])

    logging.info("Worker connecting to %s", args.endpoint)
    logging.info("Storing in %s", args.storedir)

    worker = Worker(
        args.endpoint, args.storedir,
        step_dir,
        Profiler(config.get('profile'))
        )

    await galp.cli.wait(worker.run())

    # TODO: the worker may still have submissions in queue, we should clean
    # these

    logging.info("Worker terminating normally")

class WorkerProtocol(ReplyProtocol):
    """
    Handler for messages from the broker
    """
    def __init__(self, worker, store_dir, name, router):
        super().__init__(name, router)
        self.worker = worker
        self.store = galp.cache.CacheStack(store_dir, Serializer())
        self.script = Script()
        self.commands = define_commands(self.script)

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
        logging.debug('Received GET for %s', name.hex())
        try:
            serialized = self.store.get_serial(name)
            reply = self.put(route, name, serialized)
            logging.info('GET: Cache HIT: %s', name.hex())
            return reply
        except KeyError:
            logging.info('GET: Cache MISS: %s', name.hex())
            return self.not_found(route, name)

    def on_submit(self, route, name, task_dict):
        """Start processing the submission asynchronously.

        This means returning immediately to the event loop, which allows
        processing further messages needed for the task execution (resource
        exchange, sub-tasks, ...).

        This the only asynchronous handler, all others are semantically blocking.
        """
        logging.info('SUBMIT: %s', task_dict['step_name'].decode('ascii'))

        # Store hook. For now we just check the local cache, later we'll have a
        # central locking mechanism
        # FIXME: that's probably not correct for multi-output tasks !
        if self.store.contains(name):
            logging.info('SUBMIT: Cache HIT: %s', name.hex())
            return self.done(route, name)

        # If not in cache, resolve metadata and run the task
        replies = MessageList([self.doing(route, name)])

        # If any resource is missing, send GETs back to the sender of the
        # SUBMIT. In any case, add it to the command list to be used as a
        # trigger input for the actual task execution
        for dep_name in [
            *task_dict['arg_names'],
            *task_dict['kwarg_names'].values()
            ]:
            command_key = 'GET', dep_name
            if command_key in self.script:
                # Resource was already requested at some point before, nothing
                # else to do
                continue
            if dep_name in self.store:
                # Resource is available, but we still need to mark it as such
                cdef = self.script.define_command(
                    command_key,
                    trigger=AllDone()
                    )
                self.script.run(cdef)
                continue
            # Else, we send a get back and register an external-trigger command
            replies.append(self.get(route, dep_name))
            self.script.add_command(command_key)

        self.worker.schedule_task(route, name, task_dict)

        return replies

    def on_put(self, route, name, serialized):
        """
        Put object in store, and mark the command as done
        """
        self.store.put_serial(name, serialized)
        self.script.done(('GET', name))

class Worker:
    """
    Class representing an an async worker, wrapping transport, protocol and task
    execution logic.
    """
    def __init__(self, endpoint, storedir, step_dir, profiler):
        self.protocol = WorkerProtocol(
            self, storedir,
            'BK', router=False)
        self.transport = ZmqAsyncTransport(
            self.protocol,
            endpoint, zmq.DEALER # pylint: disable=no-member
            )
        self.endpoint = endpoint
        self.step_dir = step_dir
        self.profiler = profiler

        # Submitted jobs
        self.galp_jobs = asyncio.Queue()

    def run(self):
        """
        Starts and returns life-long tasks. You should cancel the others as soon
        as any finishes or raises
        """
        tasks = [
            asyncio.create_task(self.log_heartbeat()),
            asyncio.create_task(self.monitor_jobs()),
            asyncio.create_task(self.listen())
            ]
        return tasks

    async def log_heartbeat(self):
        """
        Simple loop that periodically logs a message
        """
        i = 0
        while True:
            logging.info("Worker heartbeat %d", i)
            await asyncio.sleep(10)
            i += 1

    async def monitor_jobs(self):
        """
        Loops that waits for tasks to finsish to send back done/failed messages
        """
        while True:
            task = await self.galp_jobs.get()
            route, name, success = await task
            if success:
                reply = self.protocol.done(route, name)
            else:
                reply = self.protocol.failed(route, name)
            await self.transport.send_message(reply)

    def schedule_task(self, client_route, name, task_dict):
        """
        Callback to schedule a task for execution.

        The commands for fetching the data are assumed to have been added to the
        script already.
        """
        def _start_task(path, status):
            task = asyncio.create_task(
                self.run_submission(client_route, name, task_dict)
                )
            self.galp_jobs.put_nowait(task)
        cdef = self.protocol.script.define_command(
            ('EXEC', name),
            trigger=AllDone(
                ('GET', dep_name)
                for dep_name in [
                    *task_dict['arg_names'],
                    *task_dict['kwarg_names'].values()
                    ]
                    ),
            callback = _start_task
            )
        self.protocol.script.run(cdef)

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

    def _get_native(self, name):
        """
        Get native resource object from any available source
        """
        return self.protocol.store.get_native(name)

    async def resolve(self, step, name, arg_names, kwarg_names):
        """
        Recover handes from a step specification, the task and argument names.
        """
    async def run_submission(self, route, name, task_dict):
        """
        Actually run the task
        """
        step_name = task_dict['step_name']
        arg_names = task_dict['arg_names']
        kwarg_names = task_dict['kwarg_names']

        logging.info('Executing step %s (%s)',
            name.hex(), step_name.decode('ascii'))

        try:
            try:
                step = self.step_dir.get(step_name)
            except NoHandlerError as exc:
                logging.exception('No such step known to worker: %s', step_name.decode('ascii'))
                raise NonFatalTaskError from exc

            handle, _arg_handles, _kwarg_handles = step.make_handles(name, arg_names, kwarg_names)

            # Load args from store, usually from disk.
            try:
                args = map(self._get_native, arg_names)
                kwargs = {
                    kw.decode('ascii'): self._get_native(kwarg_name)
                    for kw, kwarg_name in kwarg_names.items()
                    }
            except KeyError as exc:
                # Could not find argument
                raise IllegalRequestError(route,
                    f'Missing dependency: {exc.args[0]}'
                    f' for step {name} ({step_name})'
                    ) from exc
            except UnicodeDecodeError as exc:
                # Either you tried to use python's non-ascii keywords feature,
                # or more likely you messed up the encoding on the caller side.
                raise IllegalRequestError(route,
                    f'Cannot decode keyword {exc.object}'
                    f' for step {name} ({step_name})'
                    ) from exc
            # This may block for a long time, by design
            try:
                result = self.profiler.wrap(name, step)(*args, **kwargs)
            except Exception as exc:
                raise NonFatalTaskError from exc

            # Store the result back
            self.protocol.store.put_native(handle, result)

            return route, name, True

        except NonFatalTaskError:
            logging.exception('Submitted task step failed: %s', step_name.decode('ascii'))
            return route, name, False
        except Exception as exc:
            # Ensures we log as soon as the error happens. The exception may be
            # re-logged afterwards.
            logging.exception('An unhandled error has occured within a step-'
                'running asynchronous task. This signals a bug in GALP itself.')
            raise

def add_parser_arguments(parser):
    """Add worker-specific arguments to the given parser"""
    parser.add_argument(
        'endpoint',
        help="Endpoint to bind to, in ZMQ format, e.g. tcp://127.0.0.2:12345 "
            "or ipc://path/to/socket ; see also man zmq_bind."
        )
    parser.add_argument('storedir')
    parser.add_argument('-c', '--config',
        help='Path to optional TOML configuration file')
    parser.add_argument('--vm',
        help='Limit on process virtual memory size, e.g. "2M" or "1G"')

    galp.cli.add_parser_arguments(parser)

if __name__ == '__main__':
    # Convenience hook to start a worker from CLI
    _parser = argparse.ArgumentParser()
    add_parser_arguments(_parser)
    asyncio.run(main(_parser.parse_args()))
