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
from galp.store import Store
from galp.protocol import Protocol
from galp.eventnamespace import EventNamespace, NoHandlerError

class IllegalRequestError(Exception):
    """Base class for all badly formed requests, triggers sending an ILLEGAL
    message back"""
    pass

def validate(condition):
    """Shortcut to raise Illegals"""
    if not condition:
        raise IllegalRequestError

class ConfigError(Exception):
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

    worker.cache = galp.cache.CacheStack(args.cachedir)
    worker.step_dir = step_dir

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
        self.store = Store(cache, self)

    async def log_heartbeat(self):
        i = 0
        while True:
            logging.warning("Worker heartbeat %d", i)
            await asyncio.sleep(10)
            i += 1

    async def send_message(self, msg):
        await self.socket.send_multipart(msg)

    async def listen(self, endpoint):
        assert self.socket is None

        ctx = zmq.asyncio.Context()
        socket = ctx.socket(zmq.DEALER)
        socket.bind(endpoint)

        self.socket = socket

        try:
            while True:
                msg = await socket.recv_multipart()
                if not len(msg):
                    logging.warning('Received illegal empty message')
                    await self.send_illegal()
                elif len(msg) == 1:
                    if msg[0] in [b'EXIT', b'ILLEGAL']:
                        logging.warning('Received %s, terminating', msg[0])
                        break
                    else:
                        logging.warning('Received illegal %s', msg[0])
                        await self.send_illegal()
                # Below this point at least two parts
                else:
                    try:
                        await self.on_message(msg)
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
        due"""
        await self.socket.send(b'ILLEGAL')

    # Protocol handlers
    # =================
    def on_invalid(self, msg):
        raise IllegalRequestError

    async def on_get(self, handle):
        logging.warning('Received GET for %s', handle.hex())
        try:
            await self.put(handle, await self.cache.get(handle))
            logging.warning('Cache Hit: %s', handle.hex())
        except KeyError:
            logging.warning('Cache Miss: %s', handle.hex())
            await self.not_found(handle)

    async def on_submit(self, handle, step_key, arg_names, kwarg_names):
        """Start processing the submission asynchronously.

        This means returning immediately to the event loop, which allows
        processing further messages needed for the task execution (resource
        exchange, sub-tasks, ...).

        This the only asynchronous handler, all others are semantically blocking.
        """

        task = asyncio.create_task(
            self.run_submission(handle, step_key, arg_names, kwarg_names)
            )

        self.tasks.append(task)

    async def on_put(self, name, obj):
        """
        Put object in store, thus releasing tasks waiting for data.
        """
        await self.store.put(name, obj)

    # Task execution logic
    # ====================

    async def load(self, name):
        """Get native resource object from any available source

        Synchronous, either returns the object or raises.
        """

    async def run_submission(self, handle, step_key, arg_names, kwarg_names):
        """
        Actually run the task if not cached.
        """

        logging.warning('Received SUBMIT for step %s', step_key)

        # End parsing, logic begins here

        # Cache hook. For now we just check the local cache, later we'll have a
        # central locking mechanism (replacing cache with store is not enough
        # for that)
        if await self.cache.contains(handle):
            logging.warning('Cache hit on SUBMIT: %s', handle.hex())
            await self.socket.send_multipart([b'DONE', handle])
            return
        
        step = self.step_dir.get(step_key)

        await self.socket.send_multipart([b'DOING', handle])

        # Load args from store, i.e. either memory, disk, or network
        try:
            keywords = kwarg_names.keys()
            kwnames = [ kwarg_names[kw] for kw in keywords ]
            all_args = await asyncio.gather(*[
                self.store.get(name) for name in (arg_names + kwnames)
                ])
            args = all_args[:len(arg_names)]
            kwargs = {
                kw.decode('ascii'): v
                for kw, v in zip(keywords, all_args[len(arg_names):])
                }
        except KeyError:
            # Could not find argument
            raise IllegalRequestError
        except UnicodeDecodeError:
            # Either you tried to use python's non-ascii keywords feature,
            # or more likely you messed up the encoding on the caller side.
            raise IllegalRequestError

        # This may block for a long time, by design
        # work work work work
        # the **kwargs syntax works even for invalid identifiers it seems ?
        try:
            result = step.function(*args, **kwargs)
        except:
            # TODO: define application errors
            raise

        # Caching
        await self.cache.put(handle, result)

        await self.socket.send_multipart([b'DONE', handle])

def add_parser_arguments(parser):
    """Add worker-specific arguments to the given parser"""
    parser.add_argument('endpoint')
    parser.add_argument('cachedir')
    parser.add_argument('-c', '--config',
        help='Path to optional TOML configuration file')

if __name__ == '__main__':
    """Convenience hook to start a worker from CLI""" 
    parser = argparse.ArgumentParser()
    add_parser_arguments(parser)
    asyncio.run(main(parser.parse_args()))
