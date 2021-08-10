"""
Worker, e.g the smallest unit that takes a job request, perform it synchronously
and returns a pass/fail notification.

Note that everything more complicated (heartbeating, load balancing, pool
management, etc) is left to upstream proxys or nannys. The only forseeable change is that
we may want to hook a progress notification, through a function callable from
within the job and easy to patch if the code has to be run somewhere else.
"""

import sys
import asyncio
import signal
import logging
import argparse
import time
import json
import importlib

import zmq
import zmq.asyncio
import toml

import galp.steps
import galp.cache
from galp.config import ConfigError
from galp.store import NetStore
from galp.protocol import Protocol
from galp.profiler import Profiler
from galp.serializer import Serializer
from galp.eventnamespace import EventNamespace, NoHandlerError

class IllegalRequestError(Exception):
    """Base class for all badly formed requests, triggers sending an ILLEGAL
    message back"""
    pass

class NonFatalTaskError(RuntimeError):
    """
    An error has occured when running a step, but the worker should keep
    running.
    """
    pass

def load_steps(plugin_names):
    step_dir = galp.steps.export
    for name in plugin_names:
        try:
            plugin = importlib.import_module(name)
            step_dir += plugin.export
            logging.warning('Loaded plug-in %s', name)
        except ModuleNotFoundError:
            logging.error('No such plug-in: %s', name)
            raise ConfigError(('No such plugin', name))
        except AttributeError:
            logging.error('Plug-in %s do not expose "export"', name)
            raise ConfigError(('Bad plugin', name))
    return step_dir

async def main(args):
    """Entry point for the worker program

    Args:
        args: an object whose attributes are the the parsed arguments, as
            returned by argparse.ArgumentParser.parse_args.
    """
    config = {}
    if args.config:
        logging.warning("Loading config file %s", args.config)
        with open(args.config) as fd:
            config = toml.load(fd)

    step_dir = load_steps(config['steps'] if 'steps' in config else [])

    # Signal handler
    terminate = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, terminate.set)
    loop.add_signal_handler(signal.SIGTERM, terminate.set)

    logging.warning("Worker starting on %s", args.endpoint)
    logging.warning("Caching to %s", args.cachedir)

    tasks = []

    worker = Worker(terminate)
    tasks.append(asyncio.create_task(worker.log_heartbeat()))

    worker.cache = galp.cache.CacheStack(args.cachedir, Serializer())
    worker.step_dir = step_dir

    worker.profiler = Profiler(config.get('profile'))

    tasks.append(asyncio.create_task(worker.listen(args.endpoint)))

    await terminate.wait()

    for task in tasks + worker.tasks:
        task.cancel()

    for task in tasks + worker.tasks:
        try:
            await task
        except asyncio.CancelledError:
            pass
        # let other exceptions be raised

    logging.warning("Worker terminating normally")

class Worker(Protocol):
    def __init__(self, terminate):
        self.terminate = terminate
        self.socket = None
        self.tasks = []

    @property
    def cache(self):
        return self._cache

    @cache.setter
    def cache(self, cache):
        self._cache = cache
        self.store = NetStore(cache, self)

    async def log_heartbeat(self):
        i = 0
        while True:
            logging.warning("Worker heartbeat %d", i)
            await asyncio.sleep(10)
            i += 1

    async def listen(self, endpoint):
        """Main message processing loop of the worker.

        Only supports one socket at once for now."""
        assert self.socket is None

        ctx = zmq.asyncio.Context()
        socket = ctx.socket(zmq.DEALER)
        socket.bind(endpoint)

        self.socket = socket

        terminate = False
        try:
            while not terminate:
                msg = await socket.recv_multipart()
                try:
                    terminate = await self.on_message(msg)
                except IllegalRequestError:
                    logging.warning('Bad request')
                    await self.send_illegal()
        finally:
            socket.close(linger=1)
            ctx.destroy()
            self.socket = None

        self.terminate.set()

    async def send_illegal(self):
        """Send a straightforward error message back so that hell is raised where
        due.

        Note: this is technically part of the Protocol and should probably be
            moved there.
        """
        await self.socket.send(b'ILLEGAL')

    # Protocol handlers
    # =================
    async def send_message(self, msg):
        await self.socket.send_multipart(msg)

    def on_invalid(self, msg):
        raise IllegalRequestError

    async def on_illegal(self):
        logging.error('Received ILLEGAL, terminating')
        return True

    async def on_exit(self):
        logging.warning('Received EXIT, terminating')
        return True

    async def on_get(self, name):
        logging.warning('Received GET for %s', name.hex())
        try:
            await self.put(name, self.cache.get_serial(name))
            logging.warning('Cache Hit: %s', name.hex())
        except KeyError:
            logging.warning('Cache Miss: %s', name.hex())
            await self.not_found(name)

    async def on_submit(self, name, step_key, arg_names, kwarg_names):
        """Start processing the submission asynchronously.

        This means returning immediately to the event loop, which allows
        processing further messages needed for the task execution (resource
        exchange, sub-tasks, ...).

        This the only asynchronous handler, all others are semantically blocking.
        """

        task = asyncio.create_task(
            self.run_submission(name, step_key, arg_names, kwarg_names)
            )

        self.tasks.append(task)

    async def on_put(self, name, obj):
        """
        Put object in store, thus releasing tasks waiting for data.
        """
        await self.store.put_serial(name, obj)

    # Task execution logic
    # ====================

    async def load(self, name):
        """Get native resource object from any available source

        Synchronous, either returns the object or raises.
        """

    async def resolve(self, step, name, arg_names, kwarg_names):
        """
        Recover handes from a step specification, the task and argument names.
        """
    async def run_submission(self, name, step_key, arg_names, kwarg_names):
        """
        Actually run the task if not cached.
        """
        try:

            logging.warning('Received SUBMIT for step %s', step_key)

            # Cache hook. For now we just check the local cache, later we'll have a
            # central locking mechanism (replacing cache with store is not enough
            # for that)
            # FIXME: that's probably not correct for multi-output tasks !
            if self.cache.contains(name):
                logging.warning('Cache hit on SUBMIT: %s', name.hex())
                await self.done(name)
                return

            # If not in cache, resolve metadata and run the task
            await self.doing(name)
            try:
                step = self.step_dir.get(step_key)
            except NoHandlerError:
                logging.exception('No such step known to worker: %s', step_key.decode('ascii'))
                raise NonFatalTaskError

            handle, arg_handles, kwarg_handles = step.make_handles(name, arg_names, kwarg_names)


            # Load args from store, i.e. either memory, disk, or network
            try:
                keywords = kwarg_handles.keys()
                kwhandles = [ kwarg_handles[kw] for kw in keywords ]
                all_args = await asyncio.gather(*[
                    self.store.get_native(in_handle) for in_handle in (arg_handles + kwhandles)
                    ])
                args = all_args[:len(arg_handles)]
                kwargs = {
                    kw.decode('ascii'): v
                    for kw, v in zip(keywords, all_args[len(arg_handles):])
                    }
            except KeyError:
                # Could not find argument
                raise IllegalRequestError
            except UnicodeDecodeError:
                # Either you tried to use python's non-ascii keywords feature,
                # or more likely you messed up the encoding on the caller side.
                raise IllegalRequestError

            # This may block for a long time, by design
            # the **kwargs syntax works even for invalid identifiers it seems ?
            try:
                result = self.profiler.wrap(name, step)(*args, **kwargs)
            except Exception as e:
                logging.exception('Submitted task step failed: %s', step_key.decode('ascii'))
                raise NonFatalTaskError

            # Caching
            self.cache.put_native(handle, result)

            await self.done(name)
        except NonFatalTaskError:
            await self.failed(name)
            raise
        except Exception as e:
            await self.failed(name)
            # Log immediately the exception
            logging.exception('An unhandled error has occured within a step-'
                'running asynchronous task. This signals a bug in GALP itself.')
            # Signal the main thread to cancel all running tasks and exit
            self.terminate.set()
            # Raise the original exception for it be caught again in the main
            # thread
            raise

def add_parser_arguments(parser):
    """Add worker-specific arguments to the given parser"""
    parser.add_argument(
        'endpoint',
        help="Endpoint to bind to, in ZMQ format, e.g. tcp://127.0.0.2:12345 "
            "or ipc://path/to/socket ; see also man zmq_bind."
        )
    parser.add_argument('cachedir')
    parser.add_argument('-c', '--config',
        help='Path to optional TOML configuration file')

if __name__ == '__main__':
    """Convenience hook to start a worker from CLI""" 
    parser = argparse.ArgumentParser()
    add_parser_arguments(parser)
    asyncio.run(main(parser.parse_args()))
