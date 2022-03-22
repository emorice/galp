"""
Broker
"""

import argparse
import asyncio
import logging

import zmq

import galp.cli

from galp.cli import IllegalRequestError
from galp.protocol import Protocol

class Broker:
    def __init__(self, terminate):
        self.client = ClientProtocol(self)
        self.worker = WorkerProtocol(self)
        self.client_socket = None
        self.worker_socket = None
        self.terminate = terminate

        self.workers = asyncio.Queue()

    async def listen_clients(self, client_endpoint):
        """Client-side message processing loop of the broker"""

        assert self.client_socket is None

        ctx = zmq.asyncio.Context()
        client_socket = ctx.socket(zmq.ROUTER)
        client_socket.bind(client_endpoint)

        self.client_socket = client_socket

        terminate = False
        logging.info("Broker listening for clients on %s", client_endpoint)
        try:
            while not terminate:
                msg = await client_socket.recv_multipart()
                try:
                    terminate = await self.client.on_message(msg)
                except IllegalRequestError as err:
                    logging.warning('Bad client request: %s', err.reason)
                    await self.client.send_illegal(err.route)
        finally:
            client_socket.close(linger=1)
            ctx.destroy()
            self.client_socket = None

        self.terminate.set()

    async def listen_workers(self, worker_endpoint):
        """Worker-side message processing loop of the broker"""

        assert self.worker_socket is None

        ctx = zmq.asyncio.Context()
        worker_socket = ctx.socket(zmq.ROUTER)
        worker_socket.bind(worker_endpoint)

        self.worker_socket = worker_socket

        terminate = False
        logging.info("Broker listening for workers on %s", worker_endpoint)
        try:
            while not terminate:
                msg = await worker_socket.recv_multipart()
                try:
                    terminate = await self.worker.on_message(msg)
                except IllegalRequestError as err:
                    logging.warning('Bad worker request: %s', err.reason)
                    await self.worker.send_illegal(err.route)
        finally:
            worker_socket.close(linger=1)
            ctx.destroy()
            self.worker_socket = None

        self.terminate.set()

class ClientProtocol(Protocol):
    """
    Handler for messages received from clients.
    """
    def __init__(self, broker):
        self.broker = broker

    async def on_submit(self, route, name, step_name, arg_names, kwarg_names):
        """
        Waits for a worker to be available and forwards the request.
        """
        worker = await self.broker.workers.get()
        self.submit(worker + route, name, step_name, arg_name, kwargs_names)

class WorkerProtocol(Protocol):
    """
    Handler for messages received from clients.
    """
    def __init__(self, broker):
        self.broker = broker

async def main(args):
    """Entry point for the broker program

    Args:
        args: an object whose attributes are the the parsed arguments, as
            returned by argparse.ArgumentParser.parse_args.
    """
    galp.cli.setup(args, "broker")

    terminate = galp.cli.create_terminate()

    broker = Broker(terminate)

    tasks = []

    tasks.append(asyncio.create_task(broker.listen_clients(args.client_endpoint)))
    tasks.append(asyncio.create_task(broker.listen_workers(args.worker_endpoint)))

    await terminate.wait()

    await galp.cli.cleanup_tasks(tasks)

    logging.info("Broker terminating normally")

def add_parser_arguments(parser):
    """Add broker-specific arguments to the given parser"""
    parser.add_argument(
        'client_endpoint',
        help="Endpoint to bind to, in ZMQ format, e.g. tcp://127.0.0.2:12345 "
            "or ipc://path/to/socket ; see also man zmq_bind. The broker will"
            "listen for incoming client connections on this endpoint."
        )
    parser.add_argument(
        'worker_endpoint',
        help="Endpoint to bind to, in ZMQ format, e.g. tcp://127.0.0.2:12345 "
            "or ipc://path/to/socket ; see also man zmq_bind. The broker will"
            "listen for incoming worker connections on this endpoint."
        )

    galp.cli.add_parser_arguments(parser)

if __name__ == '__main__':
    """Convenience hook to start a broker from CLI""" 
    parser = argparse.ArgumentParser()
    add_parser_arguments(parser)
    asyncio.run(main(parser.parse_args()))
