"""
Worker, e.g the smallest unit that takes a job request, perform it synchronously
and returns a pass/fail notification.

Note that everything more complicated (heartbeating, load balancing, pool
management, etc) is left to upstream proxys or nannys. The only forseeable change is that
we may want to hook a progress notification, through a function callable from
within the job and easy to patch if the code has to be run somewhere else.
"""

import os
import sys
import asyncio
import signal
import logging
import argparse
import resource
import time
import json
import importlib

import zmq
import zmq.asyncio
import toml

import galp.steps
import galp.cache
import galp.cli
from galp.cli import IllegalRequestError
from galp.config import ConfigError
from galp.store import NetStore
from galp.resolver import Resolver
from galp.protocol import Protocol
from galp.profiler import Profiler
from galp.serializer import Serializer, DeserializeError
from galp.eventnamespace import EventNamespace, NoHandlerError

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
            logging.info('Loaded plug-in %s', name)
        except ModuleNotFoundError:
            logging.error('No such plug-in: %s', name)
            raise ConfigError(('No such plugin', name))
        except AttributeError:
            logging.error('Plug-in %s do not expose "export"', name)
            raise ConfigError(('Bad plugin', name))
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

        soft, hard = resource.getrlimit(resource.RLIMIT_AS)
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
        with open(args.config) as fd:
            config = toml.load(fd)

    limit_resources(args)

    step_dir = load_steps(config['steps'] if 'steps' in config else [])

    logging.info("Worker connecting to %s", args.endpoint)
    logging.info("Caching to %s", args.cachedir)

    tasks = []

    worker = Worker(args.endpoint)

    worker.cache = galp.cache.CacheStack(args.cachedir, Serializer())
    worker.step_dir = step_dir

    worker.profiler = Profiler(config.get('profile'))

    await galp.cli.wait(worker.run())

    # TODO: the worker may still have submissions in queue, we should clean
    # these

    logging.info("Worker terminating normally")

class Worker(Protocol):
    def __init__(self, endpoint):
        super().__init__()
        self.endpoint = endpoint
        self.socket = None

        # Submitted jobs
        self.galp_jobs = asyncio.Queue()

    @property
    def cache(self):
        return self._cache

    @cache.setter
    def cache(self, cache):
        self._cache = cache
        self.resolver = Resolver()
        self.store = NetStore(cache, self, self.resolver)

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
        i = 0
        while True:
            logging.info("Worker heartbeat %d", i)
            await asyncio.sleep(10)
            i += 1

    async def monitor_jobs(self):
        while True:
            task = await self.galp_jobs.get()
            route, name, ok = await task
            if ok:
                await self.done(route, name)
            else:
                await self.failed(route, name)

    async def listen(self):
        """Main message processing loop of the worker.

        Only supports one socket at once for now."""
        assert self.socket is None

        ctx = zmq.asyncio.Context()
        socket = ctx.socket(zmq.DEALER)
        socket.connect(self.endpoint)

        self.socket = socket

        terminate = False
        try:
            await self.ready(self.default_route(), str(os.getpid()).encode('ascii'))
            while not terminate:
                msg = await socket.recv_multipart()
                try:
                    terminate = await self.on_message(msg)
                except IllegalRequestError as err:
                    logging.error('Bad request: %s', err.reason)
                    await self.send_illegal(err.route)
        finally:
            socket.close(linger=1)
            ctx.destroy()
            self.socket = None

        # Returning from a main task should stop the app
        return


    # Protocol handlers
    # =================
    async def send_message(self, msg):
        await self.socket.send_multipart(msg)

    def on_invalid(self, route, reason):
        raise IllegalRequestError(route, reason)

    async def on_illegal(self, route):
        logging.error('Received ILLEGAL, terminating')
        return True

    async def on_exit(self, route):
        logging.info('Received EXIT, terminating')
        return True

    async def on_get(self, route, name):
        logging.debug('Received GET for %s', name.hex())
        try:
            proto, data, children = self.cache.get_serial(name)
            await self.put(route, name, proto, data, children)
            logging.info('GET: Cache HIT: %s', name.hex())
        except KeyError:
            logging.info('GET: Cache MISS: %s', name.hex())
            await self.not_found(route, name)

    async def on_submit(self, route, name, step_key, vtags, arg_names, kwarg_names):
        """Start processing the submission asynchronously.

        This means returning immediately to the event loop, which allows
        processing further messages needed for the task execution (resource
        exchange, sub-tasks, ...).

        This the only asynchronous handler, all others are semantically blocking.
        """
        del vtags

        task = asyncio.create_task(
            self.run_submission(route, name, step_key, arg_names, kwarg_names)
            )

        await self.galp_jobs.put(task)

    async def on_put(self, route, name, proto, data, children):
        """
        Put object in store, thus releasing tasks waiting for data.
        """
        await self.store.put_serial(name, proto, data, children.to_bytes(1, 'big'))

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
    async def run_submission(self, route, name, step_key, arg_names, kwarg_names):
        """
        Actually run the task if not cached.
        """
        try:

            logging.info('SUBMIT: %s', step_key)

            # Cache hook. For now we just check the local cache, later we'll have a
            # central locking mechanism (replacing cache with store is not enough
            # for that)
            # FIXME: that's probably not correct for multi-output tasks !
            if self.cache.contains(name):
                logging.info('SUBMIT: Cache HIT: %s', name.hex())
                return route, name, True

            # If not in cache, resolve metadata and run the task
            await self.doing(route, name)
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

                # Register all resources named as routable to the sender. This means
                # that resources not available to the worker will be assumed to be
                # available on the original client.
                for in_handle in (arg_handles + kwhandles):
                    self.resolver.set_route(in_handle.name, route)

                try:
                    all_args = await asyncio.gather(*[
                        self.store.get_native(in_handle) for in_handle in (arg_handles + kwhandles)
                        ])
                except DeserializeError:
                    raise NonFatalTaskError

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
                raise NonFatalTaskError

            # Caching
            self.cache.put_native(handle, result)

            return route, name, True

        except NonFatalTaskError:
            logging.exception('Submitted task step failed: %s', step_key.decode('ascii'))
            return route, name, False
        except Exception as e:
            logging.exception('An unhandled error has occured within a step-'
                'running asynchronous task. This signals a bug in GALP itself.')
            return route, name, False

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
    parser.add_argument('--vm',
        help='Limit on process virtual memory size, e.g. "2M" or "1G"')

    galp.cli.add_parser_arguments(parser)

if __name__ == '__main__':
    """Convenience hook to start a worker from CLI"""
    parser = argparse.ArgumentParser()
    add_parser_arguments(parser)
    asyncio.run(main(parser.parse_args()))
