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

from galp import graph

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
        elif msg[0] == b'SUBMIT':
            task_name = msg[1]
            logging.warning('Received SUBMIT for task %s', task_name)
            # Insert here more detailed parsing
            handle = graph.make_handle(task_name)
            # This may block for a long time, by design
            await process_task(socket, handle)
        else:
            logging.warning('Received illegal unknown message %s', msg[0])
            await send_illegal(socket)
    
async def send_illegal(socket):
    """Send a straightforward error message back so that hell is raised where
    due"""
    await socket.send(b'ILLEGAL')

async def process_task(socket, handle):
    """

    Args:
        socket: async zmq socket were to send DONE/DOING
        handle: the parsed, but for now it's just a handle to put in messages
    """
    await socket.send_multipart([b'DOING', handle])

    # work work work work
    pass

    await socket.send_multipart([b'DONE', handle])


def add_parser_arguments(parser):
    """Add worker-specific arguments to the given parser"""
    parser.add_argument('endpoint')

if __name__ == '__main__':
    """Convenience hook to start a worker from CLI""" 
    parser = argparse.ArgumentParser()
    add_parser_arguments(parser)
    asyncio.run(main(parser.parse_args()))
