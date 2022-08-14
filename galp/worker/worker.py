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

from galp.cache import StoreReadError
from galp.graph import StepSet
from galp.config import ConfigError
from galp.lower_protocol import MessageList
from galp.protocol import ProtocolEndException, IllegalRequestError
from galp.reply_protocol import ReplyProtocol
from galp.zmq_async_transport import ZmqAsyncTransport
from galp.commands import Script
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
    Attempts to import the given modules, and add any public StepSet attribute
    to the list of currently known steps
    """
    step_dir = galp.steps.export
    for name in plugin_names:
        try:
            plugin = importlib.import_module(name)
            for k, v in vars(plugin).items():
                if isinstance(v, StepSet) and not k.startswith('_'):
                    step_dir += v
            logging.info('Loaded plug-in %s', name)
        except ModuleNotFoundError as exc:
            logging.error('No such plug-in: %s', name)
            raise ConfigError(('No such plugin', name)) from exc
        except AttributeError as exc:
            logging.error('Plug-in %s do not expose "export"', name)
            raise ConfigError(('Bad plugin', name)) from exc
    return step_dir

def limit_resources(vm=None):
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
    if vm:
        opt_suffix = vm[-1]
        exp = suffixes.get(opt_suffix)
        if exp is not None:
            mult = 10**exp
            size = int(vm[:-1]) * mult
        else:
            size = int(vm)

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
    # Early setup, make sure this work before attempting config
    galp.cli.setup("worker", config.get('debug'))
    galp.cli.set_sync_handlers()

    # Parse configuration
    config = config or {}
    configfile = config.get('configfile')
    default_configfile = 'galp.cfg'

    if not configfile and os.path.exists(default_configfile):
        configfile = default_configfile

    if configfile:
        logging.info("Loading config file %s", configfile)
        # TOML is utf-8 by spec
        with open(configfile, encoding='utf-8') as fstream:
            config.update(toml.load(fstream))

    # Validate config
    if not config.get('store'):
        raise ConfigError('No storage directory given')
    if not config.get('endpoint'):
        raise ConfigError('No endpoint directory given')

    # Late setup
    limit_resources(config.get('vm'))
    step_dir = load_steps(config.get('steps') or [])

    logging.info("Worker connecting to %s", config['endpoint'])
    logging.info("Storing in %s", config['store'])

    def _make_worker():
        return Worker(
            config['endpoint'], config['store'],
            step_dir,
            Profiler(config.get('profile'))
            )
    return _make_worker

class WorkerProtocol(ReplyProtocol):
    """
    Handler for messages from the broker
    """
    def __init__(self, worker, store_dir, name, router):
        super().__init__(name, router)
        self.worker = worker
        self.store = galp.cache.CacheStack(store_dir, Serializer())
        self.script = Script()

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

    def on_submit(self, route, task_dict):
        """Start processing the submission asynchronously.

        This means returning immediately to the event loop, which allows
        processing further messages needed for the task execution (resource
        exchange, sub-tasks, ...).

        This the only asynchronous handler, all others are semantically blocking.
        """
        logging.info('SUBMIT: %s', task_dict['step_name'].decode('ascii'))
        name = task_dict['name']

        # Store hook. For now we just check the local cache, later we'll have a
        # central locking mechanism
        # NOTE: that's probably not correct for multi-output tasks !
        if self.store.contains(name):
            logging.info('SUBMIT: Cache HIT: %s', name)
            return self.done(route, name)

        # If not in cache, resolve metadata and run the task
        replies = MessageList([self.doing(route, name)])

        # Schedule the task first. It won't actually start until its inputs are
        # marked as available, and will return the list of GETs that are needed
        cmd_rep = self.worker.schedule_task(route, name, task_dict)
        logging.info('CMD REP: %s', cmd_rep)

        # Process the list of GETs. This checks if they're in store,
        # and recursively finds new missing sub-resources when they are
        more_replies = self.command_keys_to_messages(route, cmd_rep)

        return replies + more_replies

    def on_put(self, route, name, serialized):
        """
        Put object in store, and mark the command as done
        """
        self.store.put_serial(name, serialized)
        _data, children = serialized
        cmd_rep = self.script.commands['GET', name].done(children)
        logging.info('CMD REP: %s', cmd_rep)
        return self.command_keys_to_messages(route, cmd_rep)

    def on_not_found(self, route, name):
        self.script.commands['GET', name].failed('NOTFOUND')

    def command_keys_to_messages(self, route, command_keys):
        """
        Generate galp messages from a command reply list
        """
        messages = MessageList()
        while command_keys:
            verb, name = command_keys.pop()
            if verb != 'GET':
                raise ValueError(f'Unknown command {verb}')
            try:
                children = self.store.get_children(name)
                logging.info('DEP found %s', name)
                command_keys.extend(
                    self.script.commands[verb, name].done(children)
                    )
                continue
            except StoreReadError:
                logging.exception('DEP error %s', name)
                command_keys.extend(
                    self.script.commands[verb, name].failed('StoreReadError')
                    )
                continue
            except KeyError:
                pass
            logging.info('DEP fetch %s', name)
            messages.append(
                self.get(route, name)
                )
        return messages


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
            asyncio.create_task(self.monitor_jobs()),
            asyncio.create_task(self.listen())
            ]
        return tasks

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
        """
        def _start_task(status):
            del status
            task = asyncio.create_task(
                self.run_submission(client_route, name, task_dict)
                )
            self.galp_jobs.put_nowait(task)
        init_messages = []
        self.protocol.script.callback(
            self.protocol.script.collect(
                None, init_messages, [
                    *task_dict['arg_names'],
                    *task_dict['kwarg_names'].values()
                    ]),
            callback=_start_task
            )
        return init_messages

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

    async def run_submission(self, route, name, task_dict):
        """
        Actually run the task
        """
        step_name = task_dict['step_name']
        arg_names = task_dict['arg_names']
        kwarg_names = task_dict['kwarg_names']

        logging.info('Executing step %s (%s)',
            name, step_name.decode('ascii'))

        try:
            try:
                step = self.step_dir.get(step_name)
            except NoHandlerError as exc:
                logging.exception('No such step known to worker: %s', step_name.decode('ascii'))
                raise NonFatalTaskError from exc

            handle = step.make_handle(name)

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
                logging.exception('Submitted task step failed: %s [%s]',
                step_name.decode('ascii'), name)
                raise NonFatalTaskError from exc

            # Store the result back
            self.protocol.store.put_native(handle, result)

            return route, name, True

        except NonFatalTaskError:
            # All raises include exception logging so it's safe to discard the
            # exception here
            return route, name, False
        except Exception as exc:
            # Ensures we log as soon as the error happens. The exception may be
            # re-logged afterwards.
            logging.exception('An unhandled error has occured within a step-'
                'running asynchronous task. This signals a bug in GALP itself.')
            raise

def fork(config):
    """
    Forks a worker with the given arguments.

    No validation is done on the arguments
    """
    pid = os.fork()
    if pid == 0:
        ret = 1
        try:
            ret = main(config)
        except:
            # os._exit swallows exception so make sure to log them
            logging.exception('Uncaught exception in worker')
            raise
        finally:
            # Not sys.exit after a fork as it could call parent-specific
            # callbacks
            os._exit(ret) # pylint: disable=protected-access
    return pid

def main(config):
    """
    Normal entry point
    """
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
    parser.add_argument('storedir')
    parser.add_argument('-c', '--config',
        help='Path to optional TOML configuration file')
    parser.add_argument('--vm',
        help='Limit on process virtual memory size, e.g. "2M" or "1G"')

    galp.cli.add_parser_arguments(parser)

def make_config(args):
    """
    Makes a config dictionary from parsed command-line arguments.
    """
    return {
        'endpoint': args.endpoint,
        'store': args.storedir,
        'configfile': args.config,
        'vm': args.vm
        }

def make_parser():
    """
    Creates argument parser and configures it
    """
    _parser = argparse.ArgumentParser()
    add_parser_arguments(_parser)
    return _parser
