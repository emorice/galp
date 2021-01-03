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
import zmq
import zmq.asyncio
import logging
import argparse
import time
import json

import galp.steps
from galp import graph
from galp.eventnamespace import EventNamespace, NoHandlerError

event = EventNamespace()

class IllegalRequestError(Exception):
    """Base class for all badly formed requests, triggers sending an ILLEGAL
    message back"""
    pass

# Process-wide mem cache. Not thread-safe blah blah blah
_g_mem_cache = {}
    

async def main(args):
    """Entry point for the worker program

    Args:
        args: an object whose attributes are the the parsed arguments, as
            returned by argparse.ArgumentParser.parse_args.
    """
    logging.warning("Worker starting on %s", args.endpoint)

    t = asyncio.create_task(log_heartbeat())

    await listen(args.endpoint)

    logging.warning("Worker terminating normally")

async def log_heartbeat():
    i = 0
    while True:
        logging.warning("Worker step %d", i)
        await asyncio.sleep(3)
        i += 1

async def listen(endpoint):
    ctx = zmq.asyncio.Context()
    socket = ctx.socket(zmq.DEALER)
    socket.bind(endpoint)

    while True:
        msg = await socket.recv_multipart()
        if not len(msg):
            logging.warning('Received illegal empty message')
            await send_illegal(socket)
        elif len(msg) == 1:
            if msg[0] in [b'EXIT', b'ILLEGAL']:
                logging.warning('Received %s, terminating', msg[0])
                break
            else:
                logging.warning('Received illegal %s', msg[0])
                await send_illegal(socket)
        # Below this point at least two parts
        else:
            try:
                await event.handler(str(msg[0], 'ascii'))(socket, msg)
            except NoHandlerError:
                logging.warning('No handler for event or step')
                await send_illegal(socket)
            except IllegalRequestError:
                logging.warning('Bad request')
                await send_illegal(socket)
    
async def send_illegal(socket):
    """Send a straightforward error message back so that hell is raised where
    due"""
    await socket.send(b'ILLEGAL')

def validate(condition):
    """Shortcut to raise Illegals"""
    if not condition:
        raise IllegalRequestError

@event.on('GET')
async def get(socket, msg):
    validate(len(msg) == 2)

    handle = msg[1]

    logging.warning('Received GET for %s', handle.hex())

    try:
        payload = bytes(json.dumps(_g_mem_cache[handle]), 'ascii')
        logging.warning('Cache Hit: %s', handle.hex())
        await socket.send_multipart([b'PUT', handle, payload])
    except KeyError:
        logging.warning('Cache Miss: %s', handle.hex())
        await socket.send_multipart([b'NOTFOUND', handle])
    
@event.on('SUBMIT')
async def process_task(socket, msg):
    """

    Args:
        socket: async zmq socket were to send DONE/DOING
        handle: the parsed, but for now it's just a handle to put in messages
    """
    step_name = msg[1]
    logging.warning('Received SUBMIT for step %s', step_name)

    # Collect args
    argstack = msg[2:]
    argstack.reverse()
    arg_names = []
    kwarg_names = {}
    while argstack != []:
        try:
            keyword, arg_handle = argstack.pop(), argstack.pop()
        except IndexError:
            raise IllegalRequestError
        if keyword == b'':
            arg_names.append(arg_handle)
        else:
            kwarg_names[keyword] = arg_handle

    handle = graph.Task.gen_name(step_name, arg_names, kwarg_names)

    # Cache hook:
    if handle in _g_mem_cache:
        logging.warning('Cache hit on SUBMIT: %s', handle.hex())
        await socket.send_multipart([b'DONE', handle])
        return
    
    step_dir = galp.steps.export
    step = step_dir.get(step_name)

    await socket.send_multipart([b'DOING', handle])

    # Load args, from now just from cache
    try:
        args = [ 
            _g_mem_cache[name] for name in arg_names
            ]
        kwargs = {
            kw.decode('ascii'): _g_mem_cache[name]
            for kw, name in kwarg_names.items()
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
    _g_mem_cache[handle] = result

    await socket.send_multipart([b'DONE', handle])

def add_parser_arguments(parser):
    """Add worker-specific arguments to the given parser"""
    parser.add_argument('endpoint')

if __name__ == '__main__':
    """Convenience hook to start a worker from CLI""" 
    parser = argparse.ArgumentParser()
    add_parser_arguments(parser)
    asyncio.run(main(parser.parse_args()))
