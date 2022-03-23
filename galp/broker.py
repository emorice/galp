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

        ctx = zmq.asyncio.Context()
        self.tasks_push = ctx.socket(zmq.PUSH)
        self.tasks_pull = ctx.socket(zmq.PULL)
        self.tasks_push.bind('inproc://tasks')
        self.tasks_pull.connect('inproc://tasks')

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
                # Reveive message
                msg = await client_socket.recv_multipart()
                # Parse it and handle broker-side actions
                try:
                    terminate = await self.client.on_message(msg)
                except IllegalRequestError as err:
                    logging.error('Bad client request: %s', err.reason)
                    await self.client.send_illegal(err.route)
                # Forward to worker if needed
                msg_body, route = self.worker.get_routing_parts(msg)
                incoming_route, forward_route = self.split_route(route)
                if forward_route:
                    logging.debug('Forwarding %s', msg_body[:1])
                    await self.worker.send_message_to(
                        forward_route + incoming_route,
                        msg_body)
                else:
                    logging.debug('Queuing %s', msg_body[:1])
                    await self.tasks_push.send_multipart(
                        incoming_route + [b''] + msg_body
                        )
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
                # Reveive message
                msg = await worker_socket.recv_multipart()
                # Parse it and handle broker-side actions
                try:
                    terminate = await self.worker.on_message(msg)
                except IllegalRequestError as err:
                    logging.error('Bad worker request: %s', err.reason)
                    await self.worker.send_illegal(err.route)
                # Forward to client if needed
                msg_body, route = self.worker.get_routing_parts(msg)
                incoming_route, forward_route = self.split_route(route)
                if forward_route:
                    logging.debug('Forwarding %s', msg_body[:1])
                    await self.client.send_message_to(
                        forward_route + incoming_route,
                        msg_body)
                else:
                    logging.debug('Not forwarding %s', msg_body[:1])
        finally:
            worker_socket.close(linger=1)
            ctx.destroy()
            self.worker_socket = None

        self.terminate.set()

    def split_route(self, route):
        incoming_route = route[:1]
        forward_route = route[1:]
        return incoming_route, forward_route

    async def process_tasks(self):
        """
        This event loop needs to be cancelled.
        """
        logging.debug('Starting to process task queue')
        while True:
            task = await self.tasks_pull.recv_multipart()
            task_body, task_route = self.client.get_routing_parts(task)
            logging.debug('Task pending')
            worker = await self.workers.get()
            logging.debug('Worker available, forwarding')
            await self.worker.send_message_to(worker + task_route, task_body)

class BrokerProtocol(Protocol):
    """
    Common behavior for both sides
    """
    def __init__(self, name, broker):
        super().__init__(name)
        self.broker = broker

    def on_invalid(self, route, reason):
        raise IllegalRequestError(route, reason)

    async def on_unhandled(self, verb):
        """
        For the broker, many messages will just be forwarded and no handler is
        needed.
        """
        logging.debug("No broker action for %s", verb.decode('ascii'))

class ClientProtocol(BrokerProtocol):
    """
    Handler for messages received from clients.
    """
    def __init__(self, broker):
        super().__init__('CL', broker)

    async def send_message(self, msg):
        await self.broker.client_socket.send_multipart(msg)

class WorkerProtocol(BrokerProtocol):
    """
    Handler for messages received from clients.
    """
    def __init__(self, broker):
        super().__init__('WK', broker)

    async def send_message(self, msg):
        await self.broker.worker_socket.send_multipart(msg)

    async def on_ready(self, route):
        await self.broker.workers.put(route[:1])

    async def on_done(self, route, name):
        await self.broker.workers.put(route[:1])

    async def on_failed(self, route, name):
        await self.broker.workers.put(route[:1])

    async def on_put(self, route, name, proto, data, children):
        await self.broker.workers.put(route[:1])

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
    tasks.append(asyncio.create_task(broker.process_tasks()))

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
